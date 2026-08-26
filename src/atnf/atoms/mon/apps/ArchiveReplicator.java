//
// Copyright (C) CSIRO Australia Telescope National Facility
//
// This library is free software; you can redistribute it and/or
// modify it under the terms of the GNU Library General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later version.

package atnf.atoms.mon.apps;

import java.util.*;
import atnf.atoms.mon.*;
import atnf.atoms.mon.archiver.*;
import atnf.atoms.mon.comms.*;
import atnf.atoms.time.*;

/**
 * Connects to a MoniCA server and requests bulk data from it's archive,
 * which is then used to populate a new archive. If the new archive already
 * contains some data then only more recent data will be requested from the 
 * server.
 *
 * <P>This can be used to migrate a server from one kind of archiver to
 * another or to backup/mirror an archive.
 * 
 * @author David Brodrick
 * @version $Id: ArchiveReplicator.java,v 1.1 2009/01/09 03:36:06 bro764 Exp bro764 $
 */
public
class ArchiveReplicator
{
  /** The new archive which will be populated. */
  private static PointArchiver itsNewArchive = null;
  
  /** Connection to the server. */
  private static MoniCAClient itsServer = null;

  // Command line flag values
  private static String server = null;
  private static String archiver = null;
  private static boolean backfill = false;
  
  public static final void main(String[] args) {
    List<String> argsList = new ArrayList<>(Arrays.asList(args));
    List<String> flagsList = new ArrayList<>();

    // Parse command line arguments
    for (int i = 0; i < args.length; i++) {
      if ("--server".equals(args[i]) && args.length > i + 1) {
        server = args[i + 1];
        flagsList.add(args[i]);
        flagsList.add(args[++i]);
      } else if ("--archiver".equals(args[i]) && args.length > i + 1) {
        archiver = args[i + 1];
        flagsList.add(args[i]);
        flagsList.add(args[++i]);
      } else if ("--backfill".equals(args[i])) {
        backfill = true;
        flagsList.add(args[i]);
      }
    }

    System.out.println("server: " + server + " | archiver: " + archiver + " | backfill: " + backfill);

    argsList.removeAll(flagsList);

    if (server == null || archiver == null) {
      System.err.println("USAGE: ArchiveReplicator [OPTIONS]... [point1] [pointN]");
      System.err.println("Copies a remote monitor point archive to a local archive.");
      System.err.println("If no points are specified then all points are copied.");
      System.err.println("");
      System.err.println("Arguments:");
      System.err.println("  --server    HOSTNAME    hostname of server to transfer data from");
      System.err.println("  --archiver  ARCHIVER    archiver type to transfer data to");
      System.err.println("  --backfill              backfill new archive from server data");
      System.err.println("");
      System.err.println("eg.. ArchiveReplicator --server myserver --archiver MySQL --backfill");
      System.err.println("...would copy any older data from host 'myserver' to a local MySQL archive.");
      System.exit(1);
    }
    
    //CONNECT TO SERVER
    System.out.println("#Connecting to \"" + server + "\"");
    try {
      itsServer = new MoniCAClientIce(server);
    } catch (Exception e) {
      System.err.println(e.getMessage());
      e.printStackTrace();
      System.exit(1);
    }
    
    //CREATE NEW ARCHIVER
    System.out.println("#Instanciating new PointArchiver" + archiver);
    try {
      Class archiverClass = Class.forName("atnf.atoms.mon.archiver.PointArchiver" + archiver);
      itsNewArchive = (PointArchiver)(archiverClass.newInstance());
      itsNewArchive.start();
    } catch (Exception e) {
      System.err.println("ERROR: Could not instantiate local 'PointArchiver" + archiver + "'");
      System.exit(1);
    }

    //DETERMINE WHICH POINTS TO MIGRATE
    Vector <String> serverpoints = null;
    try {
      serverpoints = itsServer.getAllPointNames();
    } catch (Exception e) {
      System.err.println("ERROR: Could not get list of point names from server: " + e.getMessage());
      System.exit(1);
    }
    Vector<String> pointnames = null;
    if (argsList.size() > 0) {
      //USER SPECIFIED SUBSET OF POINTS
      pointnames=new Vector<String>(argsList.size());
      for (int i = 0; i < argsList.size(); i++) {
        //Ensure the user-specified points exist on the server
        boolean found=false;
	for (String point: serverpoints) {
	    //        for (int j=0; j<serverpoints.length; j++) {
	    //          if (serverpoints[j].equals(args[i])) {
	    if (point.equals(argsList.get(i))) {
            found=true;
            pointnames.add(argsList.get(i));
            break;
          }
        }
        if (!found) {
          System.err.println("#ERROR: Point \"" + argsList.get(i) + "\" does not exist");
          System.exit(1);
        }
      }
    } else {
      //ALL POINTS AVAILABLE FROM SERVER
	pointnames=new Vector<String>(serverpoints.size());
      //      for (int i=0; i<serverpoints.length; i++) {
      for (String point: serverpoints) {
      pointnames.add(point);
      }
    }
    System.out.println("#Will replicate " + pointnames.size() + " points to new archive");
    
    //CREATE MONITOR POINT OBJECTS FOR EACH POINT
    Vector<PointDescription> points = null;
    try {
      points = itsServer.getPoints(pointnames);
    } catch (Exception e) {
      System.err.println("ERROR: Could not get point definitions from server: " + e.getMessage());
      System.exit(1);
    }
    
    //PROCESS EACH POINT IN TURN
    long totalrecords=0;
    for (int pointnum=0; pointnum<points.size(); pointnum++) {
      PointDescription thispoint=(PointDescription)points.get(pointnum);
      String thisname=(String)pointnames.get(pointnum);
      //IF POINT IS NOT TO BE ARCHIVED THEN DON't ARCHIVE IT
      String archivepols=thispoint.getArchivePolicyString();
      if (archivepols==null || archivepols.equals("-") || archivepols.equals("NONE")) {
        System.out.println("#Skipping non-archived point \"" + thisname + "\"");
        continue;
      }
      System.err.println("#Replicating \"" + thisname + "\"");
      
      // Determine which data to replicate to the new archiver
      AbsTime downloadstart = null;
      AbsTime downloadend = null;

      // We have two main operation modes, backfill fills in everything before
      // the earliest data available in the archive, while the default grabs
      // all the data newer than what is available in the archive.
      
      if (backfill) {
        // Find the earliest data that already exists in the archive
        downloadstart = AbsTime.factory(1l);
        PointData ourfirst=itsNewArchive.getFollowing(thispoint, downloadstart);

        if (ourfirst == null) {
          // Get everything if there is no data in the archive
          downloadend=AbsTime.factory(0l);
        } else {
          // Finish data download just before earliest data point in new archive
          System.out.println(ourfirst.toString());
          downloadend=ourfirst.getTimestamp().add(RelTime.factory(-1l));
        }
      } else {
        // Find the latest data that already exists in the archive
        downloadend = new AbsTime();
        PointData ourlast=itsNewArchive.getPreceding(thispoint, downloadend);

        // Get everything if there is no data in the archive
        if (ourlast==null) {
          downloadstart=AbsTime.factory(1l);
        } else {
          // Start data download just after latest data point in new archive
          System.out.println(ourlast.toString());
          downloadstart=ourlast.getTimestamp().add(RelTime.factory(1l));
        }
      }

      System.out.println("Starting: " + downloadstart.toString());
      System.out.println("Ending: " + downloadend.toString());

      long numcollected=0;
      while (true) {
        //COLLECT SOME MORE DATA FROM THE SERVER
        Vector<PointData> newdata=null;
        try {
          newdata = itsServer.getArchiveData(thisname, downloadstart, downloadend);
        } catch (Exception e) {
          System.err.println("ERROR: Could not communicate with server: " + e.getMessage());
          System.exit(1);
        }
        if (newdata==null || newdata.size()==0) {
          System.out.println("#Replicated " + numcollected + " data points");
          totalrecords+=numcollected;
          break;
        }
        //System.err.println("Got " + newdata.size() + " elements");
        numcollected+=newdata.size();
        
        //INSERT THIS DATA INTO LOCAL ARCHIVE
        itsNewArchive.archiveData(thispoint, newdata);          
        
        //WAIT UNTIL ARCHIVE HAS FINISHED FLUSHING
        while (itsNewArchive.checkBuffer()) {
          RelTime sleeptime=RelTime.factory(1000000l);
          try {
            sleeptime.sleep();
          } catch (Exception e) { }
          System.out.println("#Waiting for local archive to finish flushing..");
        }
//        if (numcollected>700000 || totalrecords>1000000) {
//          System.exit(0);
//        }
        
        //UPDATE QUERY TIME DELIMITERS
        downloadstart=((PointData)newdata.get(newdata.size()-1)).getTimestamp();
        downloadstart=downloadstart.add(RelTime.factory(1l));
      }
    }
    
    //DONT EXIT UNTIL LOCAL ARCHIVE HAS FINISHED FLUSHING
    while (itsNewArchive.checkBuffer()) {
      RelTime sleeptime=RelTime.factory(1000000l);
      try {
        sleeptime.sleep();
      } catch (Exception e) { }
      System.out.println("#Waiting for local archive to finish flushing..");
    }
    
    System.out.println("#Replicated " + totalrecords + " records total. Cya!");
    System.exit(0);
  }
}
