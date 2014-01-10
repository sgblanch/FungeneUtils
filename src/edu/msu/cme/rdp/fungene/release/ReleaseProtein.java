/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.msu.cme.rdp.fungene.release;

import edu.msu.cme.rdp.eutils.EUtilsSeqHolder;
import edu.msu.cme.rdp.readseq.utils.ProteinUtils;
import edu.msu.cme.rdp.readseq.utils.gbregion.RegionParser;
import edu.msu.cme.rdp.readseq.utils.gbregion.SingleSeqRegion;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.Map;

/**
 *
 * @author fishjord
 */
public class ReleaseProtein {

    int id;
    int gi;
    String protAccno;
    String nuclAccno;
    String def;
    String org;
    String pmid;
    String codedBy;
    String firstAuthor;
    int translTable;
    int codonStart;
    Float translScore;
    boolean environmental;
    int protLength;
    String protSeq;
    String nuclSeq;
    int release;
    Date addedDate;
    Date updateDate;
    Date gbDate;

    public ReleaseProtein(ResultSet rset) throws SQLException {
        id = rset.getInt("id");
        protAccno = rset.getString("prot_accno");
        nuclAccno = rset.getString("nucl_accno");
        def = rset.getString("definition");
        org = rset.getString("organism");
        firstAuthor = rset.getString("first_author");
        pmid = rset.getString("pmid");
        codedBy = rset.getString("coded_by");
        translTable = rset.getInt("transl_table");
        codonStart = rset.getInt("codon_start");
        translScore = rset.getFloat("transl_score");
        protLength = rset.getInt("prot_length");
        environmental = rset.getBoolean("environmental");

        protSeq = rset.getString("prot_seq");
        nuclSeq = rset.getString("nucl_seq");

        addedDate = rset.getDate("added_date");
        updateDate = rset.getDate("updated_date");

        gi = rset.getInt("gi");
    }

    public ReleaseProtein(EUtilsSeqHolder seq, Map<String, String> cds) throws NumberFormatException, IOException {
        Date gbCreateDate = seq.getReleaseDate();
        Date gbUpdateDate = seq.getUpdateDate();
        if (gbCreateDate.after(gbUpdateDate)) {
            gbDate = gbCreateDate;
        } else {
            gbDate = gbUpdateDate;
        }

        if(cds.containsKey("product")) {
            def = cds.get("product");
        } else {
            def = seq.getDefinition();
        }
        org = seq.getOrganism();
        id = -1;
        gi = Integer.valueOf(seq.getGi());
        environmental = seq.isEnvironmental();

        EUtilsSeqHolder.TerseReference ref = seq.getFirstReference();
        firstAuthor = ref.getFirstAuthorName();
        pmid = ref.getPmid();

        protAccno = cds.get("protein_id").split("\\.")[0];

        if (protAccno == null) {
            throw new IOException("No protein accession number in gb seq");
        }

        nuclAccno = seq.getPrimaryAccession().split("\\.")[0];
        if(nuclAccno == null) {
            throw new IOException("Nucleotide accno is null");
        }

        if (cds.containsKey("codon_start")) {
            codonStart = Integer.valueOf(cds.get("codon_start")) - 1;
        } else {
            codonStart = 0;
        }

        if (cds.containsKey("transl_table")) {
            translTable = Integer.valueOf(cds.get("transl_table"));
        } else {
            translTable = 11;
        }

        SingleSeqRegion region = null;
	try {
	    region = RegionParser.parse(cds.get("location"));
	} catch(IOException e) {
	    throw new IOException("Failed to parse location: " + cds.get("location") + ", error was: " + e.getMessage(), e);
	}
        codedBy = cds.get("location");

        nuclSeq = region.getSeqRegion(seq.getSeqString()).toLowerCase().substring(codonStart);
        int nuclLength = nuclSeq.length();
        if(nuclLength % 3 != 0) {
            nuclSeq = nuclSeq.substring(0, nuclLength - nuclLength%3);
        }

        protSeq = cds.get("translation").toLowerCase();

        if (cds.containsKey("translation")) {
            translScore = ProteinUtils.getInstance().getTranslScore(protSeq, nuclSeq, region, translTable);
        }

        protLength = protSeq.length();

        release = -1;
        addedDate = null;
        updateDate = null;
    }
}
