/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.msu.cme.rdp.fungene.utils;

import edu.msu.cme.rdp.fungene.db.FungeneDB;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 *
 * @author fishjord
 */
public class FungeneStats extends FungeneDB {

    private class HmmProteinKey {
        Integer hmmId;
        String proteinKey;
    }

    private FungeneProps props;

    private int totalNRSeqs;
    private int newSeqs;
    private int totalSeqs;
    private int newModels;
    private int totalModels;
    private int deletedUnalignedProtSeqs;
    private int deletedUnalignedNucSeqs;
    private int deletedAlignedProtSeqs;
    private int deletedAlignedNucSeqs;
    private int lowTranslScores;
    private int extremeHmmScores;
    private int noCodedByThisRelease;
    private int missingUnalignedSeqs;
    private int missingTranslScore;
    private int missingHmmScore;
    private int missingPfamScores;

    public FungeneStats(FungeneProps props) {
        super(props);
        this.props = props;

        Connection dbConn = null;
        Statement stmt = null;
        ResultSet rset = null;

        try {
            dbConn = ds.getConnection();
            dbConn.setAutoCommit(false);

            stmt = dbConn.createStatement();

            deletedUnalignedProtSeqs = stmt.executeUpdate("delete from unaligned_prot_sequence where not exists(select 1 from protein where seq_id=unaligned_prot_seq_id)");
            deletedUnalignedNucSeqs = stmt.executeUpdate("delete from unaligned_nuc_sequence where not exists(select 1 from protein where seq_id=unaligned_nuc_seq_id);");
            deletedAlignedProtSeqs = stmt.executeUpdate("delete from aligned_prot_sequence where not exists(select 1 from hmm_model_protein_record where seq_id=aligned_prot_seq_id);");
            deletedAlignedNucSeqs = stmt.executeUpdate("delete from aligned_nuc_sequence where not exists(select 1 from protein_aligned_nucl_seq where seq_id=nucl_seq_id);");

            lowTranslScores = firstAsInt(stmt, "select count(*) from protein where nuc_transl_score < .99");
            extremeHmmScores = getHmmOutliers(stmt);

            totalModels = firstAsInt(stmt, "select count(*) from hmm_model;");
            newModels = firstAsInt(stmt, "select count(*) from hmm_model where added_in_release=" + props.getReleaseNo());

            totalNRSeqs = firstAsInt(stmt, "select count(*) from nonredundant_mapping");
            totalSeqs = firstAsInt(stmt, "select count(*) from protein");
            newSeqs = firstAsInt(stmt, "select count(*) from hmm_model_protein_record where added_in_release=" + props.getReleaseNo());

            missingPfamScores = firstAsInt(stmt, "select count(*) from motif_model_protein_record where score < 0");

            noCodedByThisRelease = firstAsInt(stmt, "select count(*) from protein where coded_by is null");
            missingUnalignedSeqs = firstAsInt(stmt, "select count(*) from protein where unaligned_prot_seq_id is null or unaligned_nuc_seq_id is null");

            missingTranslScore = firstAsInt(stmt, "select count(*) from protein where nuc_transl_score is null");
            missingHmmScore = firstAsInt(stmt, "select count(*) from hmm_model_protein_record where hmm_score is null and added_in_release=" + props.getReleaseNo());

            dbConn.commit();
        } catch(SQLException e) {
            try {
               dbConn.rollback();
            } catch(Exception ee) {}
            throw new RuntimeException(e);
        } finally {
            try {
                rset.close();
            } catch(Exception e) {}
            try {
                stmt.close();
            } catch(Exception e) {}
            try {
                dbConn.close();
            } catch(Exception e) {}
            try {
                ds.close();
            } catch(Exception e) {}
        }
    }

    private static int firstAsInt(Statement stmt, String query) throws SQLException {
        ResultSet rset = null;
        try {
            rset = stmt.executeQuery(query);
            rset.next();
            return rset.getInt(1);
        } finally {
            try {
                rset.close();
            } catch (Exception e) {}
        }

    }

    private int getHmmOutliers(Statement stmt) throws SQLException {
        stmt.execute("create temporary table tmp_hmm_stats (hmm_id int primary key, lb float, ub float) on commit drop;");
        stmt.execute("insert into tmp_hmm_stats (select hmm_id, avg(hmm_score) - 3 * stddev_samp(hmm_score), avg(hmm_score) + 3 * stddev_samp(hmm_score) from hmm_model_protein_record group by hmm_id)");

        return firstAsInt(stmt, "select count(*) from hmm_model_protein_record where not exists (select 1 from tmp_hmm_stats where tmp_hmm_stats.hmm_id=hmm_model_protein_record.hmm_id and lb < hmm_score and ub > hmm_score)");
    }

    public void printStats() throws IOException {
        printStats(new PrintStream(props.getStatsFile()));
    }

    public void printStats(PrintStream out) throws IOException {

        out.println("Release\t" + props.getReleaseNo());
        out.println("Total non-redundent proteins\t" + totalNRSeqs);
        out.println("Total proteins\t" + totalSeqs);
        out.println("New proteins-hmm model pairs\t" + newSeqs);
        out.println("Total models\t" + totalModels);
        out.println("New models\t" + newModels);
        out.println("Unreferenced unaligned prot seqs\t" + deletedUnalignedProtSeqs);
        out.println("Unreferenced unaligned nuc seqs\t" + deletedUnalignedNucSeqs);
        out.println("Unreferenced aligned prot seqs\t" + deletedAlignedProtSeqs);
        out.println("Unreferenced aligned nuc seqs\t" + deletedAlignedNucSeqs);
        out.println("Proteins with low nucleotide translation scores (score < .99)\t" + lowTranslScores);
        out.println("Aligned proteins with extreme hmm scores (score > 3 std deviations away)\t" + extremeHmmScores);
        out.println("Proteins missing coded by\t" + noCodedByThisRelease);
        out.println("Proteins missing unaligned sequences\t" + missingUnalignedSeqs);
        out.println("Proteins missing translation scores\t" + missingTranslScore);
        out.println("Motifs missing bits saved scores\t" + missingPfamScores);
        out.println("Proteins missing hmm scores\t" + missingHmmScore);

        out.close();
    }

    public static void main(String [] args) throws Exception {
        new FungeneStats(FungeneProps.loadProps(new File("default_fg_props.xml"))).printStats(System.out);
    }
}
