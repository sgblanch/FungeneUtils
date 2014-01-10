/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.msu.cme.rdp.fungene.cli;

import edu.msu.cme.rdp.eutils.EUtilsSeqFetcher;
import edu.msu.cme.rdp.readseq.utils.gbregion.RegionParser;
import edu.msu.cme.rdp.readseq.utils.gbregion.SingleSeqRegion;
import edu.msu.cme.rdp.readseq.writers.FastaWriter;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.PrintStream;
import java.util.Date;

/**
 *
 * @author fishjord
 */
public class NucleotideFetcher {

    public static void main(String[] args) throws Exception {
        if(args.length != 3) {
            System.err.println("USAGE: NucleotideFetcher <cds_mapping> <nucl_out> <fetch_errors.txt>");
            System.exit(1);
        }

        BufferedReader reader = new BufferedReader(new FileReader(new File(args[0])));
        String line;

        EUtilsSeqFetcher fetcher = new EUtilsSeqFetcher();
        FastaWriter out = new FastaWriter(args[1]);
        PrintStream err = new PrintStream(args[2]);

        long startTime = System.currentTimeMillis();
        try {
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.equals("")) {
                    continue;
                }

                String[] lexemes = line.split("\\s+");
                String protGi = lexemes[0];
                String codedBy = lexemes[2];
                Integer codonStart = Integer.valueOf(lexemes[3]);

                if (codedBy.equals("null")) {
                    continue;
                }

                SingleSeqRegion region = RegionParser.parse(codedBy);
                try {
                    String seqString = fetcher.fetchRegionByUrl(region).toLowerCase().substring(codonStart-1);

                    out.writeSeq(region.getId(), "prot_gi=" + protGi, seqString);
                } catch (Exception e) {
                    err.println(protGi + "\t" + e.getMessage());
                }
            }
        } finally {
            err.close();
            out.close();
            reader.close();
        }

        System.out.println("Nucleotide sequences fetched in " + (System.currentTimeMillis() - startTime) + " ms at " + new Date());
    }
}
