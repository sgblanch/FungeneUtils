/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package edu.msu.cme.rdp.fungene.cli;

import java.util.Date;

/**
 *
 * @author fishjord
 */
public class PfamHelper {

    public static class PfamHMMHeader {
        private String hmmerVersion;
        private String accno;
        private String motifName;
        private int version;
        private int length;
        private int seedSeqs;
        private Date createdOn;

        public PfamHMMHeader(String hmmerVersion, String motifName, String accno, int version, int length, int seedSeqs, Date createdOn) {
            this.hmmerVersion = hmmerVersion;
            this.motifName = motifName;
            this.accno = accno;
            this.version = version;
            this.length = length;
            this.seedSeqs = seedSeqs;
            this.createdOn = createdOn;
        }

        public String getAccno() {
            return accno;
        }

        public Date getCreatedOn() {
            return createdOn;
        }

        public String getHmmerVersion() {
            return hmmerVersion;
        }

        public int getLength() {
            return length;
        }

        public int getSeedSeqs() {
            return seedSeqs;
        }

        public int getVersion() {
            return version;
        }

        public String getMotifName() {
            return motifName;
        }
    }

}
