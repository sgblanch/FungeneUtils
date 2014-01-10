/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.msu.cme.rdp.fungene.db;

import edu.msu.cme.rdp.fungene.utils.SequenceType;
import edu.msu.cme.rdp.fungene.utils.UseAsIds;
import edu.msu.cme.rdp.readseq.readers.Sequence;
import edu.msu.cme.rdp.readseq.utils.SeqUtils;
import edu.msu.cme.rdp.readseq.utils.gbregion.SingleSeqRegion;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author fishjord
 */
public class DetailedSequence implements Serializable {

    private int protGi;
    private String protAccno;
    private String nucAccno;
    private String alignedProtSeq;
    private String alignedNucSeq;
    private String organism;
    private String definition;
    private SingleSeqRegion codedBy;
    private int translTable;
    private float nuclTranslScore;
    private float hmmCoverage;
    private int fungeneId;

    public DetailedSequence(int pid, int protGi, String protAccno, String nuclAccno, String alignedProtSeq,
            String alignedNucSeq, String organism, String definition,
            SingleSeqRegion codedBy, int translTable, float nuclTranslScore,
            float hmmCoverage) {
        this.protGi = protGi;
        this.protAccno = protAccno;
        this.alignedProtSeq = alignedProtSeq;
        this.alignedNucSeq = alignedNucSeq;
        this.organism = organism;
        this.definition = definition;
        this.codedBy = codedBy;
        this.translTable = translTable;
        this.nuclTranslScore = nuclTranslScore;
        this.hmmCoverage = hmmCoverage;
        this.nucAccno = nuclAccno;
        this.fungeneId = pid;
    }

    public int getFungeneId() {
        return fungeneId;
    }

    public String getAlignedNucSeq() {
        return alignedNucSeq;
    }

    public String getAlignedProtSeq() {
        return alignedProtSeq;
    }

    public String getModelNucSeq() {
        return alignedNucSeq.replaceAll("[a-z\\.]", "");
    }

    public String getModelProtSeq() {
        return alignedProtSeq.replaceAll("[a-z\\.]", "");
    }

    public String getUnalignedNucSeq() {
        if (alignedNucSeq == null) {
            return null;
        }
        return SeqUtils.getUnalignedSeqString(alignedNucSeq);
    }

    public String getUnalignedProtSeq() {
        return SeqUtils.getUnalignedSeqString(alignedProtSeq);
    }

    public String getNucAccno() {
        return nucAccno;
    }

    public String getProtAccno() {
        return protAccno;
    }

    public int getProtGi() {
        return protGi;
    }

    public String getDefinition() {
        return definition;
    }

    public String getOrganism() {
        return organism;
    }

    void setAlignedProtSeq(String seq) {
        this.alignedProtSeq = seq;
    }

    void setAlignedNucSeq(String seq) {
        this.alignedNucSeq = seq;
    }

    public SingleSeqRegion getCodedBy() {
        return codedBy;
    }

    public float getHmmCoverage() {
        return hmmCoverage;
    }

    public float getNuclTranslScore() {
        return nuclTranslScore;
    }

    public int getTranslTable() {
        return translTable;
    }

    public Sequence toSequence(SequenceType type, UseAsIds id, boolean aligned) {
        String desc = "organism=" + organism + ",definition=" + definition;
        if (type == SequenceType.Protein) {
            String strId = protAccno;
            if (id == UseAsIds.gi) {
                strId = protGi + "";
            } else if (id == UseAsIds.name) {
                strId = organism.replaceAll("\\s+", "_");
            }

            desc = ((codedBy == null) ? "" : "coded_by=" + codedBy.toString()) + "," + desc;

            if (aligned) {
                return new Sequence(strId, desc, getAlignedProtSeq());
            } else {
                return new Sequence(strId, desc, getUnalignedProtSeq());
            }
        } else if (type == SequenceType.Nucleotide) {
            if (alignedNucSeq == null) {
                return null;
            }

            String strId = nucAccno;
            if (id == UseAsIds.gi) {
                strId = protGi + "";
            } else if (id == UseAsIds.name) {
                strId = organism.replaceAll("\\s+", "_");
            }

            desc = "location=" + codedBy.toString() + "," + desc;

            if (aligned) {
                return new Sequence(strId, desc, getAlignedNucSeq());
            } else {
                return new Sequence(strId, desc, getUnalignedNucSeq());
            }
        }

        return null;
    }

    public Sequence toModelSequence(SequenceType type) {
        if (type == SequenceType.Protein) {
            return new Sequence(fungeneId + "", protAccno, getModelProtSeq());
        } else if (type == SequenceType.Nucleotide) {
            if (alignedNucSeq == null) {
                return null;
            }
            return new Sequence(fungeneId + "", nucAccno, getModelNucSeq());
        }

        return null;
    }

    public static List<Sequence> toSequences(List<DetailedSequence> seqs, SequenceType seqType, boolean aligned) {
        return toSequences(seqs, UseAsIds.accno, seqType, aligned);
    }

    public static List<Sequence> toSequences(List<DetailedSequence> seqs, UseAsIds id, SequenceType seqType, boolean aligned) {
        List<Sequence> ret = new ArrayList();
        for (DetailedSequence seq : seqs) {
            Sequence toadd = seq.toSequence(seqType, id, aligned);
            if (toadd != null) {
                ret.add(toadd);
            }
        }
        return ret;
    }

    public static List<Sequence> toModelSequences(List<DetailedSequence> seqs, SequenceType seqType) {
        List<Sequence> ret = new ArrayList();
        for (DetailedSequence seq : seqs) {
            Sequence toadd = seq.toModelSequence(seqType);
            if (toadd != null) {
                ret.add(toadd);
            }
        }
        return ret;
    }
}
