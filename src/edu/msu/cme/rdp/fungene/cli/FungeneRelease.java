/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.msu.cme.rdp.fungene.cli;

import edu.msu.cme.rdp.fungene.utils.FungeneProps;
import edu.msu.cme.rdp.fungene.utils.FungeneStats;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import javax.xml.bind.JAXBException;

/**
 *
 * @author fishjord
 */
public class FungeneRelease {

    private static String[] subcmds = new String[]{
        "prerelease - starts a release (args: <release_name> [release_desc]>)",
        "update - fetches any necessary pfam and nr updates",
        "index - re-indexes the nr file, this is done automatically by update",
        "scan - starts hmmscan across the nr db [gridware]",
        "fetch - starts fetching new proteins and nucleotide records",
        "align-prot - aligns any protein sequences that need to be aligned [gridware]",
        "align-nuc - aligns any nucleotide sequences that need to be aligned (based on protein alignment)",
        "motif-scan - scans proteins for any missing motif scores",
        "env - sets enviornmental status on sequences (args: env file from qiong)",
        "postrelease - runs post release checks",
        "check-updates - checks for pfam/nr updates",
        "check-nonredundant - checks for pfam/nr updates",
        "help - displays usage"
    };

    private static void printUsage() throws IOException {
        System.out.println("USAGE: " + FungeneRelease.class.getCanonicalName() + " <fungene_props> <subcommand> <subcommand args...>");
        System.out.println("Where sub commands are:");
        for (String subcmd : subcmds) {
            System.out.println("    " + subcmd);
        }
    }

    private static void prerelease(File fungenePropsFile, String releaseName, String releaseDesc) throws IOException, JAXBException {

        FungeneProps props = FungeneProps.loadProps(fungenePropsFile);


        System.out.print("Begining pre-release directory creation for release " + releaseName + " (desc= " + releaseDesc + ") in ");
        for (int index = 5; index >= 0; index--) {
            System.out.print(index + "..");

            try {
                Thread.sleep(1000);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        System.out.println();
        fungenePropsFile = PrereleaseHelper.prereleaseSetup(props, releaseName, releaseDesc);
        PrintStream log = props.getLogStream();

        System.out.println("Prerelease setup for release " + props.getReleaseNo() + " (" + releaseName + ") complete");
        log.println("Prerelease setup for release " + props.getReleaseNo() + " (" + releaseName + ") complete");

        log.close();

    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2 || args[1].equals("help")) {
            printUsage();
            return;
        }
        if (args.length < 2) {
            System.out.println("XML Property file must be specified");
            return;
        }

        String command = args[1];
        File fungenePropsFile = new File(args[0]).getAbsoluteFile();
        if (!fungenePropsFile.exists()) {
            System.err.println("Properties file \"" + args[1] + "\" does not exist");
            return;
        }

        if (command.equals("prerelease")) {
            if (args.length < 3) {
                System.err.println("Please specify a release name, and optionally description");
                return;
            }

            String releaseName = args[2];
            String releaseDesc = "";

            if (args.length > 3) {
                releaseDesc = args[3];
            }

            prerelease(fungenePropsFile, releaseName, releaseDesc);
        } else if (command.equals("postrelease")) {
            FungeneStats stats = new FungeneStats(FungeneProps.loadProps(fungenePropsFile));
            stats.printStats(System.out);
            stats.printStats();
        } else {
            printUsage();
        }
    }
}
