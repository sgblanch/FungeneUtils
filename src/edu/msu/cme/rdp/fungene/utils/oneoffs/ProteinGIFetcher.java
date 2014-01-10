/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.msu.cme.rdp.fungene.utils.oneoffs;


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
 * @author wangqion
 */
public class ProteinGIFetcher {
    
    public static void main(String[] args) throws Exception {
        if ( args.length != 2){
            throw new IllegalArgumentException("Usage: gi.txt prot_region.txt");
        }
        Set<String> gis = new HashSet();
        File giFile = new File(args[0]);
        
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
        
        PrintStream outPrint = new PrintStream(args[1]);
        EUtilsSeqFetcher fetcher = new EUtilsSeqFetcher();
        FastaWriter out = new FastaWriter(args[1]);
        try {
            System.out.println("Begining fetch of " + toFetch.size() + "  sequence records at " + new Date());
            long startTime = System.currentTimeMillis();
            int fetchsize = 200;
           // for (int index = 0; index < toFetch.size(); index += EUtilsSeqFetcher.MAX_PER_FETCH) {
            for (int index = 0; index < toFetch.size(); index += fetchsize) {
                //int end = index + EUtilsSeqFetcher.MAX_PER_FETCH;
                int end = index + fetchsize;
                
                if (end > gis.size()) {
                    end = gis.size();
                }
System.err.println("now fetch " + index + " end=" + end + " " + toFetch.get(index).toString());
                Set<EUtilsSeqHolder> set = fetcher.fetchRecords(toFetch.subList(index, end), "nucleotide");
                //Set<EUtilsSeqHolder> set = fetcher.fetchSingle(toFetch.get(index), "nucleotide", null);
                for (EUtilsSeqHolder seq : set) {
                    String gi = seq.getGi();

                    String codedBy = null;
                    
                    outPrint.println( gi + "\t" + seq.getPrimaryAccession() );

                  //  out.writeSeq(protAccno, "gi=" + gi+ ", organism=" + organism + ", definition=" +  definition, seq.getINSDSeqSequence() );
                   // outPrint.println(gi + "\t" + codedBy + "\t" + codonStart + "\t" + translTable + "\t" + organism + "\t" + definition);
                }
            }

            System.out.println("Protein sequences fetched in " + (System.currentTimeMillis() - startTime) + " ms at " + new Date());
        } finally {
            out.close();
        }
    }
}
