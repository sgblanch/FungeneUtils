/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.msu.cme.rdp.fungene.release;

import edu.msu.cme.rdp.fungene.db.FungeneDB;
import edu.msu.cme.rdp.fungene.utils.FungeneProps;
import edu.msu.cme.rdp.readseq.readers.SeqReader;
import edu.msu.cme.rdp.readseq.readers.Sequence;
import edu.msu.cme.rdp.readseq.readers.SequenceReader;
import java.io.File;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author fishjord
 */
public class MarkSeeds extends FungeneDB {

    private final Connection dbConn;
    private final PreparedStatement findProtId;
    private final PreparedStatement findProtIdBySeq;
    private final PreparedStatement markSeed;
    private final Statement stmt;
    private final Map<String, Integer> hmmMap = new HashMap();

    public MarkSeeds(FungeneProps props) throws SQLException {
        super(props);
        dbConn = ds.getConnection();
        findProtId = dbConn.prepareStatement("select prot_seqid from protein where prot_accno=? or nucl_accno=?");
	findProtIdBySeq = dbConn.prepareStatement("select seqid from unaligned_prot_sequence where crc32=? and seq=?");
        markSeed = dbConn.prepareStatement("update hmm_protein set is_seed=true where hmm_id=? and pid=?");

        stmt = dbConn.createStatement();
        ResultSet rset = stmt.executeQuery("select hmm_name, hmm_id from hmm");
        while(rset.next()) {
            hmmMap.put(rset.getString("hmm_name"), rset.getInt("hmm_id"));
        }
        rset.close();
    }

    public static void main(String[] args) throws Exception{
        if(args.length < 2) {
            System.err.println("USAGE: MarkSeeds <fungene_props> <seed file>...");
            System.exit(1);
        }

        FungeneProps props = FungeneProps.loadProps(new File(args[0]));
        MarkSeeds marker = new MarkSeeds(props);
	ResultSet rset = null;

        for(int index = 1;index < args.length;index++) {
            File seedFile = new File(args[index]);
            String hmmName = seedFile.getName();
            hmmName = hmmName.substring(0, hmmName.indexOf("."));

            Integer hmmId = marker.hmmMap.get(hmmName);
            if(hmmId == null) {
                System.err.println("Cannot find a model with name'" + hmmName + "'");
                continue;
            }

            SeqReader reader = new SequenceReader(seedFile);
            Sequence seq;
            while((seq = reader.readNextSequence()) != null) {
                String seqName = seq.getSeqName();
                if(seqName.contains("|")) {
		    String[] lexemes = seqName.split("\\|");

		    seqName = null;
		    for(int i = 0;i < lexemes.length - 1;i++) {
			if(lexemes[i].equals("emb") || lexemes[i].equals("dbj") || lexemes[i].equals("gb") || lexemes[i].equals("ref")) {
			    seqName = lexemes[i + 1];
			    break;
			}
		    }

		    if(seqName == null) {
			System.err.println("Couldn't find an accession number in seqname " + seq.getSeqName());
			continue;
		    }
		}

		if(seqName.contains(".")) {
		    seqName = seqName.substring(0, seqName.indexOf("."));
		}

		if(seqName.indexOf("_") != seqName.lastIndexOf("_")) {
		    seqName = seqName.substring(0, seqName.lastIndexOf("_"));
		}

		marker.findProtId.setString(1, seqName);
		marker.findProtId.setString(2, seqName);
		rset = marker.findProtId.executeQuery();
		Integer protId = null;
		try {
		    if(rset.next()) {
			protId = rset.getInt("prot_seqid");

			if(rset.next()) {
			    System.err.println("More than one result returned (Weeeeeeeird) for sequence " + seqName);
			    continue;
			}
		    } else {
			rset.close();
			String seqString = seq.getSeqString().toLowerCase();
			marker.findProtIdBySeq.setLong(1, ReleasePipeline.getCRC32(seqString));
			marker.findProtIdBySeq.setString(2, seqString);
			rset = marker.findProtIdBySeq.executeQuery();
			if(rset.next()) {
			    protId = rset.getInt("seqid");
			} else {
			    rset.close();
			    rset = marker.stmt.executeQuery("select seqid from unaligned_prot_sequence where seq ilike '%" + seqString + "%'");

			    if(rset.next()) {
				System.err.println("Found seq " + seq.getSeqName() + " by inexact match");
				protId = rset.getInt("seqid");
			    } else {
				System.err.println("Couldn't find prot id for sequence " + seq.getSeqName() + " (tried lookup by seqid, seq, and inexact seq)");
				continue;
			    }
			}
		    }
		} finally {
		    rset.close();
		}

		System.out.println(seqName + "\t" + hmmName + "\t" + hmmId + "\t" + protId);
		marker.markSeed.setInt(1, hmmId);
		marker.markSeed.setInt(2, protId);
		marker.markSeed.addBatch();
            }
        }

	marker.markSeed.executeBatch();
    }
}
