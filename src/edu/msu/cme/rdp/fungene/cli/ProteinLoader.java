/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.msu.cme.rdp.fungene.cli;

import edu.msu.cme.rdp.eutils.EUtilsSeqFetcher;
import edu.msu.cme.rdp.eutils.EUtilsSeqHolder;
import edu.msu.cme.rdp.readseq.writers.FastaWriter;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 *
 * @author fishjord
 */
public class ProteinLoader {
    public static void main(String[] args) throws Exception {

        if(args.length != 3) {
            System.err.println("USAGE: ProteinFetcher <accno_file> <mapping_out> <prot_seq_out>");
            System.exit(1);
        }

        Set<String> gis = new HashSet();
        File giFile = new File(args[0]);
        File mappingOutFile = new File(args[1]);
        File protSeqOut = new File(args[2]);

        BufferedReader reader = new BufferedReader(new FileReader(giFile));
        String line;

        while((line = reader.readLine()) != null) {
            line = line.trim();
            if(!line.equals("")) {
                gis.add(line);
            }
        }

        reader.close();

        List<String> toFetch = new ArrayList(gis);

        PrintStream out = new PrintStream(mappingOutFile);
        EUtilsSeqFetcher fetcher = new EUtilsSeqFetcher();
        FastaWriter seqout = new FastaWriter(protSeqOut);

        try {
            System.out.println("Begining fetch of " + toFetch.size() + " protein sequence records at " + new Date());
            long startTime = System.currentTimeMillis();
            for (int index = 0; index < toFetch.size(); index += EUtilsSeqFetcher.MAX_PER_FETCH) {
                int end = index + EUtilsSeqFetcher.MAX_PER_FETCH;
                if (end > gis.size()) {
                    end = gis.size();
                }

                Set<EUtilsSeqHolder> set = fetcher.fetchRecords(toFetch.subList(index, end));
                for (EUtilsSeqHolder seq : set) {
                    String gi = seq.getGi();

                    String codedBy = null;
                    String protAccno = seq.getPrimaryAccession();
                    String organism = seq.getOrganism();
                    String definition = seq.getDefinition();
                    int codonStart = seq.getFirstCodonStart();
                    int translTable = seq.getFirstTranslTable();

                    try {
                        codedBy = seq.getFirstSeqRegion().toString();
                    } catch(Exception ignore) {}

                    out.println(gi + "\t" + protAccno + "\t" + codedBy + "\t" + codonStart + "\t" + translTable + "\t" + organism + "\t" + definition);
                    seqout.writeSeq(protAccno, seq.getSeqString());
                }
            }

            System.err.println("Protein sequences fetched in " + (System.currentTimeMillis() - startTime) + " ms at " + new Date());
        } finally {
            seqout.close();
            out.close();
        }
    }
}
