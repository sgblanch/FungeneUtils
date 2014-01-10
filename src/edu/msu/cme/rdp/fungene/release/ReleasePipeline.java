/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.msu.cme.rdp.fungene.release;

import edu.msu.cme.rdp.eutils.EUtilsSeqHolder;
import edu.msu.cme.rdp.alignment.hmm.jni.HMMER3;
import edu.msu.cme.rdp.alignment.hmm.jni.HMMER3Hit;
import edu.msu.cme.rdp.fungene.db.FungeneDB;
import edu.msu.cme.rdp.fungene.utils.FungeneProps;
import edu.msu.cme.rdp.fungene.utils.ProteinUtils;
import edu.msu.cme.rdp.readseq.utils.SeqUtils;
import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;

/**
 *
 * WARNING: This class is -NOT- thread safe
 *
 * @author fishjord
 */
public class ReleasePipeline extends FungeneDB {

    private static class AlignedProt {

        int hmmId;
        String alignedProt;
    }

    private static class HMM {

        int hmmId, modelLength;
        String modelName;
    }
    private final Connection dbConn;
    private final PreparedStatement protSelect;
    private final PreparedStatement protIns;
    private final PreparedStatement protUpdate;
    private final PreparedStatement hmmProteinPrepStmt;
    private final PreparedStatement unalignedNuclIns;
    private final PreparedStatement unalignedProtIns;
    private final PreparedStatement unalignedNuclSelect;
    private final PreparedStatement unalignedProtSelect;
    private final PreparedStatement alignedNuclIns;
    private final PreparedStatement alignedProtIns;
    private final PreparedStatement alignedNuclSelect;
    private final PreparedStatement alignedProtSelect;
    private final PreparedStatement protAlignmentSelect;
    private final PreparedStatement protAlignedNuclIns;
    private final PreparedStatement newProtAlignedNuclIns;
    private static final ProteinUtils protUtils = ProteinUtils.getInstance();
    private Map<String, HMM> hmmsByNames = new HashMap();
    private Map<Integer, HMM> hmmsById = new HashMap();

    public ReleasePipeline(FungeneProps props) throws SQLException {
        super(props);
        dbConn = ds.getConnection();
        protSelect = dbConn.prepareStatement("select id, prot_accno, "
                + "nucl_accno, definition, organism, first_author, pmid,"
                + " coded_by, transl_table, codon_start, transl_score, "
                + "prot_length, environmental, gi, "
                + "(select seq from unaligned_nucl_sequence where seqid = nucl_seqid) as nucl_seq, "
                + "(select seq from unaligned_prot_sequence where seqid = prot_seqid) as prot_seq, "
                + "(select release_date from release where release_id = added_in_release) as added_date, "
                + "(select release_date from release where release_id = updated_in_release) as updated_date "
                + "from protein "
                + "where prot_accno=?");

        protIns = dbConn.prepareStatement("insert into protein (prot_accno,"
                + "nucl_accno,definition,organism,first_author,pmid,coded_by,"
                + "transl_table,codon_start,transl_score,prot_length,prot_seqid,"
                + "nucl_seqid,added_in_release,updated_in_release, genbank_date, environmental, gi)"
                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?"
                + ", ?, ?, ?, ?)");

        protUpdate = dbConn.prepareStatement("update protein set prot_accno=?,"
                + "nucl_accno=?,definition=?,organism=?,first_author=?,pmid=?,coded_by=?,"
                + "transl_table=?,codon_start=?,"
                + "updated_in_release=?, genbank_date=?, environmental=?, gi=?"
                + " WHERE id=?");

        hmmProteinPrepStmt = dbConn.prepareStatement("insert into hmm_protein "
                + "(hmm_id, pid, added_in_release, bits_saved, hmm_start, hmm_end, hmm_coverage, seq_start, seq_end, aligned_prot_seqid) "
                + "VALUES "
                + "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

        unalignedNuclIns = dbConn.prepareStatement("insert into unaligned_nucl_sequence (seq, crc32) values (?, ?)");
        unalignedProtIns = dbConn.prepareStatement("insert into unaligned_prot_sequence (seq, crc32) values (?, ?)");
        unalignedProtSelect = dbConn.prepareStatement("select seqid from unaligned_prot_sequence where crc32=? and seq=?");
        unalignedNuclSelect = dbConn.prepareStatement("select seqid from unaligned_nucl_sequence where crc32=? and seq=?");

        alignedNuclIns = dbConn.prepareStatement("insert into aligned_nucl_sequence (seq, crc32) values (?, ?)");
        alignedProtIns = dbConn.prepareStatement("insert into aligned_prot_sequence (seq, crc32) values (?, ?)");
        alignedProtSelect = dbConn.prepareStatement("select seqid from aligned_prot_sequence where crc32=? and seq=?");
        alignedNuclSelect = dbConn.prepareStatement("select seqid from aligned_nucl_sequence where crc32=? and seq=?");

        protAlignmentSelect = dbConn.prepareStatement("select hmm_id, (select seq from aligned_prot_sequence where seqid = aligned_prot_seqid) as aligned_prot from hmm_protein where pid=?");

        protAlignedNuclIns = dbConn.prepareStatement("insert into protein_aligned_nucl_seq values(?, ?, ?)");
        newProtAlignedNuclIns = dbConn.prepareStatement("insert into protein_aligned_nucl_seq values(currval('prot_id_seq'), ?, ?)");

        Statement stmt = dbConn.createStatement();
        ResultSet rset = stmt.executeQuery("select * from hmm");

        while (rset.next()) {
            HMM hmm = new HMM();
            hmm.hmmId = rset.getInt("hmm_id");
            hmm.modelLength = rset.getInt("hmm_length");
            hmm.modelName = rset.getString("hmm_name");
            hmmsByNames.put(hmm.modelName, hmm);
            hmmsById.put(hmm.hmmId, hmm);
        }

        rset.close();
        stmt.close();
    }

    public void close() throws SQLException {
        protSelect.close();
        protIns.close();
        protUpdate.close();

        protAlignmentSelect.close();

        hmmProteinPrepStmt.close();
        unalignedNuclIns.close();
        unalignedProtIns.close();
        unalignedNuclSelect.close();
        unalignedProtSelect.close();

        alignedNuclIns.close();
        alignedProtIns.close();
        alignedNuclSelect.close();
        alignedProtSelect.close();

        protAlignedNuclIns.close();
        newProtAlignedNuclIns.close();

        dbConn.close();
    }

    private ReleaseProtein getProtein(String protAccno) throws SQLException {
        ResultSet rset = null;
        try {
            protSelect.setString(1, protAccno);

            rset = protSelect.executeQuery();

            if (!rset.next()) {
                return null;
            }

            return new ReleaseProtein(rset);

        } finally {
            try {
                rset.close();
            } catch (Exception e) {
            }
        }
    }

    private void insertProt(ReleaseProtein p, int nuclSeqid, int protSeqid, int release) throws SQLException {
        protIns.setString(1, p.protAccno);
        protIns.setString(2, p.nuclAccno);
        protIns.setString(3, p.def);
        protIns.setString(4, p.org);
        protIns.setString(5, p.firstAuthor);
        protIns.setString(6, p.pmid);
        protIns.setString(7, p.codedBy);
        protIns.setInt(8, p.translTable);
        protIns.setInt(9, p.codonStart);
        if (p.translScore != null) {
            protIns.setFloat(10, p.translScore);
        } else {
            protIns.setNull(10, Types.FLOAT);
        }
        protIns.setInt(11, p.protLength);
        protIns.setInt(12, protSeqid);
        protIns.setInt(13, nuclSeqid);

        protIns.setInt(14, release);
        protIns.setInt(15, release);
        protIns.setDate(16, new java.sql.Date(p.gbDate.getTime()));
        protIns.setBoolean(17, p.environmental);
        protIns.setInt(18, p.gi);

        protIns.execute();
    }

    private void updateProtein(ReleaseProtein p, int pid, int release) throws SQLException {
        try {
            dbConn.setAutoCommit(false);
            protUpdate.setString(1, p.protAccno);
            protUpdate.setString(2, p.nuclAccno);
            protUpdate.setString(3, p.def);
            protUpdate.setString(4, p.org);
            protUpdate.setString(5, p.firstAuthor);
            protUpdate.setString(6, p.pmid);
            protUpdate.setString(7, p.codedBy);
            protUpdate.setInt(8, p.translTable);
            protUpdate.setInt(9, p.codonStart);

            protUpdate.setInt(10, release);
            protUpdate.setDate(11, new java.sql.Date(p.gbDate.getTime()));
            protUpdate.setBoolean(12, p.environmental);
            protUpdate.setInt(13, p.gi);

            protUpdate.setInt(14, pid);

            protUpdate.execute();
            dbConn.commit();
        } catch (SQLException e) {
            dbConn.rollback();
            throw e;
        }
    }

    private void insertSeq(String seq, Long crc, boolean prot, boolean aligned) throws SQLException {
        if (crc == null) {
            crc = getCRC32(seq);
        }

        PreparedStatement stmt;

        if (prot) {
            if (aligned) {
                stmt = alignedProtIns;
            } else {
                stmt = unalignedProtIns;
            }
        } else {
            if (aligned) {
                stmt = alignedNuclIns;
            } else {
                stmt = unalignedNuclIns;
            }
        }

        stmt.setString(1, seq);
        stmt.setLong(2, crc);
        stmt.execute();
    }

    private Integer findSeqid(String seq, Long crc, boolean prot, boolean aligned) throws SQLException {
        if (crc == null) {
            crc = getCRC32(seq);
        }

        PreparedStatement stmt = null;
        ResultSet rset = null;
        try {
            if (prot) {
                if (aligned) {
                    stmt = alignedProtSelect;
                } else {
                    stmt = unalignedProtSelect;
                }
            } else {
                if (aligned) {
                    stmt = alignedNuclSelect;
                } else {
                    stmt = unalignedNuclSelect;
                }
            }

            stmt.setLong(1, crc);
            stmt.setString(2, seq);
            rset = stmt.executeQuery();

            if (!rset.next()) {
                return null;
            }

            return rset.getInt("seqid");
        } finally {
            rset.close();
        }
    }

    private void insertProtAlignments(List<HMMER3Hit> hits, int protSeqid, int release) throws SQLException {
        HMM hmm;
        int inserted = 0;
        float coverage;
        for (HMMER3Hit hit : hits) {
            String alignedSeq = hit.getAlignedSeq();
            hmm = hmmsByNames.get(hit.getModelName());

            if (hmm == null) {
                throw new SQLException("No hmm with name " + hit.getModelName());
            }

            coverage = (float) (hit.getHmmEnd() - hit.getHmmStart()) / hmm.modelLength;

            if (SeqUtils.countModelPositions(alignedSeq) != hmm.modelLength) {
                System.err.println(SeqUtils.countModelPositions(alignedSeq) + ": " + alignedSeq);
                System.err.println(hmm.modelName + ": " + hmm.modelLength);
                System.err.println();
                throw new SQLException("Aligned sequence length != model length");
            }

            long crc = getCRC32(alignedSeq);
            insertSeq(alignedSeq, crc, true, true);
            int seqid = findSeqid(alignedSeq, crc, true, true);

            hmmProteinPrepStmt.setInt(1, hmm.hmmId);
            hmmProteinPrepStmt.setInt(2, protSeqid);
            hmmProteinPrepStmt.setInt(3, release);
            hmmProteinPrepStmt.setDouble(4, hit.getBits());
            hmmProteinPrepStmt.setInt(5, hit.getHmmStart());
            hmmProteinPrepStmt.setInt(6, hit.getHmmEnd());
            hmmProteinPrepStmt.setFloat(7, coverage);
            hmmProteinPrepStmt.setInt(8, hit.getSeqStart());
            hmmProteinPrepStmt.setInt(9, hit.getSeqEnd());
            hmmProteinPrepStmt.setInt(10, seqid);
            hmmProteinPrepStmt.addBatch();

            inserted++;
        }

        hmmProteinPrepStmt.executeBatch();
    }

    private void updateNuclAlignments(ReleaseProtein p, int protSeqid, int pid, int release) throws SQLException {
        for (AlignedProt prot : getProtAlignments(protSeqid)) {
            int lenDiff = (p.protLength * 3) - p.nuclSeq.length();

            if (lenDiff != 0 && (p.protSeq.charAt(p.protLength - 1) == '*'
                    && lenDiff != 1)) {
                throw new SQLException("Length different of protein and nucl sequence for protein " + p.protAccno);
            }

            String alignedNucl = protUtils.getAlignedNucSeq(prot.alignedProt, p.nuclSeq, true, 11);
            long crc = getCRC32(alignedNucl);
            insertSeq(alignedNucl, crc, false, true);
            int alignedNuclSeqid = findSeqid(alignedNucl, crc, false, true);

            protAlignedNuclIns.setInt(1, pid);
            protAlignedNuclIns.setInt(2, alignedNuclSeqid);
            protAlignedNuclIns.setInt(3, prot.hmmId);
            protAlignedNuclIns.addBatch();
        }
        protAlignedNuclIns.executeBatch();
    }

    public void updateNuclAlignments(String protSeq, String nuclSeq, int protSeqid, int pid) throws SQLException {
        dbConn.setAutoCommit(false);
        try {
            for (AlignedProt prot : getProtAlignments(protSeqid)) {
                int lenDiff = (protSeq.length() * 3) - nuclSeq.length();

                if (lenDiff != 0 && (protSeq.charAt(protSeq.length() - 1) == '*'
                        && lenDiff != 1)) {
                    throw new SQLException("Length different of protein and nucl sequence for protein " + pid);
                }

                String alignedNucl = protUtils.getAlignedNucSeq(prot.alignedProt, nuclSeq, true, 11);
                long crc = getCRC32(alignedNucl);
                insertSeq(alignedNucl, crc, false, true);
                int alignedNuclSeqid = findSeqid(alignedNucl, crc, false, true);

                protAlignedNuclIns.setInt(1, pid);
                protAlignedNuclIns.setInt(2, alignedNuclSeqid);
                protAlignedNuclIns.setInt(3, prot.hmmId);
                protAlignedNuclIns.addBatch();
            }
            protAlignedNuclIns.executeBatch();
            dbConn.commit();
        } catch (SQLException e) {
            dbConn.rollback();
            throw e;
        } finally {
            dbConn.setAutoCommit(false);
        }
    }

    private List<AlignedProt> getProtAlignments(int protSeqid) throws SQLException {
        List<AlignedProt> ret = new ArrayList();
        protAlignmentSelect.setInt(1, protSeqid);
        ResultSet rset = protAlignmentSelect.executeQuery();

        try {
            while (rset.next()) {
                AlignedProt alignedProt = new AlignedProt();
                alignedProt.hmmId = rset.getInt("hmm_id");
                alignedProt.alignedProt = rset.getString("aligned_prot");

                ret.add(alignedProt);
            }
        } finally {
            try {
                rset.close();
            } catch (Exception e) {
            }
        }

        return ret;
    }

    private void insertProtein(ReleaseProtein p, List<HMMER3Hit> hits, int release) throws SQLException {
        long nuclCRC32 = getCRC32(p.nuclSeq);
        long protCRC32 = getCRC32(p.protSeq);

        dbConn.setAutoCommit(false);

        if (hits != null) {
            insertSeq(p.protSeq, protCRC32, true, false);
        }

        insertSeq(p.nuclSeq, nuclCRC32, false, false);

        int nuclSeqid = findSeqid(p.nuclSeq, nuclCRC32, false, false);
        int protSeqid = findSeqid(p.protSeq, protCRC32, true, false);

        insertProt(p, nuclSeqid, protSeqid, release);

        if (hits != null) {
            insertProtAlignments(hits, protSeqid, release);
        }
        for (AlignedProt prot : getProtAlignments(protSeqid)) {
            int lenDiff = (p.protLength * 3) - p.nuclSeq.length();

            if (lenDiff != 0 && (p.protSeq.charAt(p.protLength - 1) == '*'
                    && lenDiff != 1)) {
                throw new SQLException("Length different of protein and nucl sequence for protein " + p.protAccno);
            }

            String alignedNucl = protUtils.getAlignedNucSeq(prot.alignedProt, p.nuclSeq, true, 11);
            long crc = getCRC32(alignedNucl);
            insertSeq(alignedNucl, crc, false, true);
            int alignedNuclSeqid = findSeqid(alignedNucl, crc, false, true);

            newProtAlignedNuclIns.setInt(1, alignedNuclSeqid);
            newProtAlignedNuclIns.setInt(2, prot.hmmId);
            newProtAlignedNuclIns.addBatch();
        }
        newProtAlignedNuclIns.executeBatch();

        dbConn.commit();
        dbConn.setAutoCommit(true);
    }
    private static final CRC32 crc32 = new CRC32();

    public static long getCRC32(String seq) {
        crc32.reset();
        crc32.update(seq.getBytes());
        return crc32.getValue();
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1 && args.length != 2) {
            System.err.println("USAGE: ReleasePipeline <props> [force_rescan]");
            System.exit(1);
        }

        FungeneProps props = FungeneProps.loadProps(new File(args[0]));
        ReleasePipeline pipeline = new ReleasePipeline(props);
        EUtilsSeqHolder seq;
        HMMER3 hmmer = new HMMER3(props.getHmmDbFile().getAbsolutePath(), props.getLibraryPath());
        boolean forceRescan = (args.length != 1 && (args[1].equals("true") || args[1].equals("yes")));

        int readSeqs = 0;
        int readCDS = 0;
        int insProt = 0;
        long startTime = System.currentTimeMillis();

        EntrezXMLReader reader = new EntrezXMLReader(System.in);
        while ((seq = reader.readNext()) != null) {
            readSeqs++;

            List<Map<String, String>> cdss = seq.getFeatures("cds", null, null);
            if (cdss.size() > 0) {
                for (Map<String, String> cds : cdss) {
                    if (!cds.containsKey("translation")) {
                        continue;
                    }
                    readCDS++;
                    try {
                        ReleaseProtein gbProtein = new ReleaseProtein(seq, cds);
                        ReleaseProtein dbProtein = pipeline.getProtein(gbProtein.protAccno);
                        Integer protSeqid = pipeline.findSeqid(gbProtein.protSeq, null, true, false);

                        if (dbProtein != null && gbProtein.gbDate.after(dbProtein.updateDate)) {

                            if (!gbProtein.protSeq.equals(dbProtein.protSeq) || !gbProtein.nuclSeq.equals(dbProtein.nuclSeq)) {
                                System.err.println(gbProtein.protAccno + "\tfatal\tUpdated version available but sequences don't match...please fix me");
                                continue;
                            }

                            System.err.println(gbProtein.protAccno + "\tinfo\tUpdating to latest gb release version");
                            pipeline.updateProtein(gbProtein, dbProtein.id, props.getReleaseNo());
                        }

                        List<HMMER3Hit> goodHits = null;
                        if (forceRescan || protSeqid == null) {
                            HMMER3Hit[] hits = hmmer.findHits(gbProtein.protSeq);
                            goodHits = new ArrayList();
                            for (HMMER3Hit hit : hits) {
                                double seqCoverage = (float) (hit.getSeqEnd() - hit.getSeqStart()) / gbProtein.protLength;
                                if (hit.getBits() > props.getMinBitsSaved() && seqCoverage > props.getMinSeqCoverage()) {
                                    goodHits.add(hit);
                                }
                            }
                        }

                        if (goodHits == null || goodHits.size() > 0) {
                            insProt++;

                            try {
                                if (dbProtein == null) {
                                    pipeline.insertProtein(gbProtein, goodHits, props.getReleaseNo());
                                } else if (goodHits != null && goodHits.size() > 0) {
                                    pipeline.dbConn.setAutoCommit(false);
                                    pipeline.insertProtAlignments(goodHits, protSeqid, props.getReleaseNo());
                                    pipeline.updateNuclAlignments(gbProtein, protSeqid, dbProtein.id, props.getReleaseNo());
                                    pipeline.dbConn.commit();
                                    pipeline.dbConn.setAutoCommit(true);
                                }
                            } catch (SQLException e) {
                                System.err.println("Insert of " + gbProtein.protAccno + " (" + gbProtein.nuclAccno + ") failed: " + e.toString());

                                if (e.getNextException() != null) {
                                    e.getNextException().printStackTrace();
                                    System.exit(1);
                                }
                            }
                        }

                    } catch (SQLException e) {
                        System.err.println("Error creating protein object from " + seq.getPrimaryAccession() + ": " + e.getMessage());
                        e.printStackTrace();

                        if (e.getNextException() != null) {
                            e.getNextException().printStackTrace();
                            System.exit(1);
                        }
                    } catch (Exception e) {
                        System.err.println(seq.getPrimaryAccession() + "\tfatal\t" + e.getMessage());
                    }
                }
            }

            if (readSeqs % 10000 == 0) {
                System.err.println("Processed " + readSeqs + " in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
            }
        }

        System.out.println("Read in " + readSeqs + " sequences, with " + readCDS + " cds features and inserted " + insProt + " protein sequences in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

        pipeline.close();
        reader.close();
    }
}
