/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.msu.cme.rdp.fungene.utils;

import edu.msu.cme.rdp.readseq.utils.gbregion.Extends;
import edu.msu.cme.rdp.readseq.utils.gbregion.SingleSeqRegion;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author fishjord
 */
public class ProteinUtils {

    public static class AminoAcid {

        private Character aminoAcid;
        private boolean initiator;

        public AminoAcid(Character aminoAcid, boolean initiator) {
            this.aminoAcid = Character.toLowerCase(aminoAcid);
            this.initiator = initiator;
        }

        public Character getAminoAcid() {
            return aminoAcid;
        }

        public Character getAminoAcid(int index) {
            return aminoAcid;
        }

        public boolean isInitiator() {
            return initiator;
        }

        public boolean matches(char c) {
            return aminoAcid == Character.toLowerCase(c);
        }
    }
    private Map<Integer, Map<String, AminoAcid>> translationTableMap = new LinkedHashMap();
    private static final Pattern tableIdPattern = Pattern.compile("transl_table=(\\d+)");
    private static final Pattern tableEntryPattern = Pattern.compile("([A-Z]{3})\\s+([A-Z*]{1})\\s+[A-Z][a-z]{2}(\\s+i)?");

    private static class ProteinUtilsHolder {

        private static ProteinUtils holder = new ProteinUtils();
    }

    private static class TranslateResult {

        int errors;
        String translStr;
    }

    private ProteinUtils() {
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(this.getClass().getResourceAsStream("/data/protein_coding_tables.txt")));
            String line;
            Integer currTable = -1;
            Map<String, AminoAcid> translationTable = new LinkedHashMap();

            while ((line = reader.readLine()) != null) {
                Matcher m = tableIdPattern.matcher(line);

                if (m.find()) {
                    if (currTable != -1) {
                        translationTableMap.put(currTable, translationTable);
                    }

                    currTable = new Integer(m.group(1));
                    translationTable = new LinkedHashMap();
                } else {
                    m = tableEntryPattern.matcher(line);

                    while (m.find()) {
                        translationTable.put(m.group(1), new AminoAcid(m.group(2).charAt(0), m.group().endsWith("i")));
                    }
                }
            }

            if (!translationTableMap.containsKey(currTable)) {
                translationTableMap.put(currTable, translationTable);
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public int proteinTables() {
        return translationTableMap.size();
    }

    public Map<String, AminoAcid> getTranslationTable(int translTable) {
        return translationTableMap.get(translTable);
    }

    public static ProteinUtils getInstance() {
        return ProteinUtilsHolder.holder;
    }

    public String translateToProtein(String unalignedNucSeq, boolean dontAllowInitiators, int translTable) {
        Map<String, AminoAcid> proteinMapping = translationTableMap.get(translTable);
        if (proteinMapping == null) {
            throw new IllegalArgumentException("No such protein translation table " + translTable);
        }

        StringBuffer ret = new StringBuffer();

        for (int index = 0; index < unalignedNucSeq.length(); index += 3) {
            if (index + 3 > unalignedNucSeq.length()) {
                break;
            }

            String codonTrip = unalignedNucSeq.substring(index, index + 3).toUpperCase().replace("U", "T");
            if (codonTrip.matches("[ACTG]{3}")) {
                AminoAcid code = proteinMapping.get(codonTrip.toUpperCase());
                if (index == 0 && !dontAllowInitiators && code.isInitiator()) {
                    ret.append('m');
                } else {
                    ret.append(code.getAminoAcid());
                }
            } else {
                ret.append('x');
            }

        }

        return ret.toString();
    }

    private TranslateResult backTranslate(String protSeq, String unalignedNucSeq, boolean dontAllowInitiators, int translTable) {
        Map<String, AminoAcid> proteinMapping = translationTableMap.get(translTable);
        if (proteinMapping == null) {
            throw new IllegalArgumentException("No such protein translation table " + translTable);
        }

        int unalignedIndex = 0;
        int errors = 0;
        StringBuffer nucSeq = new StringBuffer();

        for (int index = 0; index < protSeq.length(); index++) {
            char protBase = protSeq.charAt(index);

            if (protBase == 'x' || protBase == 'X') {
                if (Character.isUpperCase('x')) {
                    nucSeq.append("NNN");
                } else {
                    nucSeq.append("nnn");
                }
                unalignedIndex++;
            } else if (Character.isLetter(protBase)) {
                int nucSeqIndex = unalignedIndex * 3;
                char p = protBase;

                if (nucSeqIndex + 3 > unalignedNucSeq.length()) {
                    break;
                }

                String codonTrip = unalignedNucSeq.substring(nucSeqIndex, nucSeqIndex + 3).toUpperCase().replace("U", "T");
                if (codonTrip.matches("[ACTG]{3}")) {
                    AminoAcid code = proteinMapping.get(codonTrip.toUpperCase());
                    char aminoAcid = code.getAminoAcid();
                    if (index == 0 && !dontAllowInitiators && code.isInitiator()) {
                        aminoAcid = 'm';
                    }

                    if (code == null) {
                        errors++;
                        //System.out.println(codonTrip + " doesn't code for anything");
                    } else if (code.matches('*') && index + 1 != protSeq.length()) {
                        //System.out.println(codonTrip + " is a stop codon");
                        if (index + 1 != protSeq.length()
                                && Character.toLowerCase(p) != 'u') {
                            errors++;
                        }
                        
                        if(p != '*') {
                            unalignedIndex++;
                        }
                    } else if (Character.toLowerCase(aminoAcid) != Character.toLowerCase(p)) {
                        errors++;
                        //System.out.println(codonTrip + " " + Character.toLowerCase(aminoAcid) + " != " + Character.toLowerCase(p));
                    }
                }

                if (Character.isUpperCase(protBase)) {
                    nucSeq.append(codonTrip.toUpperCase());
                } else {
                    nucSeq.append(codonTrip.toLowerCase());
                }

                unalignedIndex++;
            } else if (protBase == '-') {
                nucSeq.append("---");
            } else if (protBase == '.') {
                nucSeq.append("...");
            }
        }

        TranslateResult result = new TranslateResult();
        result.errors = errors;
        result.translStr = nucSeq.toString();
        return result;
    }

    public String getAlignedNucSeq(String alignedProtSeq, String unalignedNucSeq, SingleSeqRegion r, int translTable) {
        return backTranslate(alignedProtSeq, unalignedNucSeq, (r.getExtends() == Extends.BEYOND_BEGIN || r.getExtends() == Extends.BEYOND_BOTH), translTable).translStr;
    }

    public String getAlignedNucSeq(String alignedProtSeq, String unalignedNucSeq, boolean dontAllowInitiators, int translTable) {
        return backTranslate(alignedProtSeq, unalignedNucSeq, dontAllowInitiators, translTable).translStr;
    }

    public float getTranslScore(String proteinSeq, String nucSeq, SingleSeqRegion r, int translTable) {
        int errors = backTranslate(proteinSeq, nucSeq, (r.getExtends() == Extends.BEYOND_BEGIN || r.getExtends() == Extends.BEYOND_BOTH), translTable).errors;
        return ((proteinSeq.length() - errors) / (float) proteinSeq.length());
    }

    public float getTranslScore(String proteinSeq, String nucSeq, boolean dontAllowInitiators, int translTable) {
        int errors = backTranslate(proteinSeq, nucSeq, dontAllowInitiators, translTable).errors;
        return ((proteinSeq.length() - errors) / (float) proteinSeq.length());
    }

    public static void main(String[] args) {
        Map<Integer, Map<String, AminoAcid>> transMap = ProteinUtils.getInstance().translationTableMap;
        for (Integer i : transMap.keySet()) {
            System.out.println(i);
            Map<String, AminoAcid> aaMap = transMap.get(i);
            for (String codon : aaMap.keySet()) {
                System.out.println(codon + "\t" + aaMap.get(codon).aminoAcid + "\t" + aaMap.get(codon).isInitiator());
            }
        }
    }
}
