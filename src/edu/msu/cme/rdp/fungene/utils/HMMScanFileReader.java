/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.msu.cme.rdp.fungene.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

/**
 *
 * @author fishjord
 */
public class HMMScanFileReader {

    public static class HMMScanLine {

        private String hmmName;
        private String seqName;
        private float bitsSaved;
        private float eval;

        public HMMScanLine(String hmmName, String seqName, float bitsSaved, float eval) {
            this.hmmName = hmmName;
            this.seqName = seqName;
            this.bitsSaved = bitsSaved;
            this.eval = eval;
        }

        public float getBitsSaved() {
            return bitsSaved;
        }

        public float getEval() {
            return eval;
        }

        public String getHmmName() {
            return hmmName;
        }

        public String getSeqName() {
            return seqName;
        }
    }
    private BufferedReader reader;

    public HMMScanFileReader(File f) throws IOException {
        reader = new BufferedReader(new FileReader(f));
    }

    public HMMScanLine nextResult() throws IOException {
        String line;

        while ((line = reader.readLine()) != null) {
            if (line.startsWith("#")) {
                continue;
            }

            String[] tokens = line.trim().split("\\s+");
            if (tokens.length < 2) {
                continue;
            }

            String hmmName = tokens[0].split("\\.")[0];
            String seqName = tokens[2];
            float eval = new Float(tokens[4]);
            float bitsSaved = new Float(tokens[5]);
            return new HMMScanLine(hmmName, seqName, bitsSaved, eval);
        }

        return null;
    }

    public void close() throws IOException {
        reader.close();
    }
}
