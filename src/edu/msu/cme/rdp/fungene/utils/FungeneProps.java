/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package edu.msu.cme.rdp.fungene.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.URL;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

/**
 *
 * @author fishjord
 */
@XmlRootElement
public class FungeneProps {

    private static final int XML_VERSION = 6;
    public static final String ALIGN_DIR = "to_align";
    public static final String RELEASE_STATS_FILE = "stats.txt";
    public static final String CODED_BY_FILE = "coded_by";
    public static final String HMM_DB_NAME = "all_models.hmm";
    public static final String RELEASE_LOG = "release.log";
    public static final String GRIDWARE_LOG = "gridware.log";

    public static class DataSourceProps {
        private String dbUrl;
        private String userName;
        private String password;

        public String getDbUrl() {
            return dbUrl;
        }

        public void setDbUrl(String dbUrl) {
            this.dbUrl = dbUrl;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public String getUserName() {
            return userName;
        }

        public void setUserName(String userName) {
            this.userName = userName;
        }
    }

    private Integer releaseNo;
    private int minBitsSaved;
    private float minSeqCoverage;
    private String baseDir;
    private String hmmAlignCmd;
    private String hmmDir;
    private String motifDir;
    private boolean verbose;
    private int xmlVersion;
    private URL nrFtpUrl;
    private URL pfamUrl;
    private String libraryPath;

    private DataSourceProps dbProps;

    public String getHmmDir() {
        return hmmDir;
    }

    public void setHmmDir(String hmmDir) {
        this.hmmDir = hmmDir;
    }

    public String getMotifDir() {
        return motifDir;
    }

    public void setMotifDir(String motifDir) {
        this.motifDir = motifDir;
    }

    public Integer getReleaseNo() {
        return releaseNo;
    }

    public void setReleaseNo(Integer releaseNo) {
        this.releaseNo = releaseNo;
    }

    public int getMinBitsSaved() {
        return minBitsSaved;
    }

    public void setMinBitsSaved(int minBitsSaved) {
        this.minBitsSaved = minBitsSaved;
    }

    @XmlAttribute
    public int getXmlVersion() {
        return xmlVersion;
    }

    public void setXmlVersion(int xmlVersion) {
        this.xmlVersion = xmlVersion;
    }

    public String getBaseDir() {
        return baseDir;
    }

    public void setBaseDir(String baseDir) {
        this.baseDir = baseDir;
    }

    public DataSourceProps getDbProps() {
        return dbProps;
    }

    public void setDbProps(DataSourceProps props) {
        this.dbProps = props;
    }

    public String getHmmAlignCmd() {
        return hmmAlignCmd;
    }

    public void setHmmAlignCmd(String hmmAlignCmd) {
        this.hmmAlignCmd = hmmAlignCmd;
    }

    public boolean isVerbose() {
        return verbose;
    }

    public void setVerbose(boolean verbose) {
        this.verbose = verbose;
    }

    public URL getPfamUrl() {
        return pfamUrl;
    }

    public void setPfamUrl(URL url) {
        this.pfamUrl = url;
    }

    public URL getNrFtpUrl() {
        return nrFtpUrl;
    }

    public void setNrFtpUrl(URL url) {
        this.nrFtpUrl = url;
    }

    public float getMinSeqCoverage() {
        return minSeqCoverage;
    }

    public void setMinSeqCoverage(float minSeqCoverage) {
        this.minSeqCoverage = minSeqCoverage;
    }

    @XmlTransient
    public File getWorkDir() {
        if(getReleaseNo() == null)
            throw new IllegalArgumentException("Looks like this is a stub property file, can't get release number");
        return new File(getBaseDir(), getReleaseNo() + "").getAbsoluteFile();
    }

    @XmlTransient
    public File getHmmDbFile() {
        return new File(getHmmDir(), HMM_DB_NAME).getAbsoluteFile();
    }

    @XmlTransient
    public PrintStream getLogStream() throws IOException {
        File f = new File(getWorkDir(), RELEASE_LOG);
        if(!f.exists())
            return new PrintStream(new FileOutputStream(f));
        else
            return new PrintStream(new FileOutputStream(f, true));
    }

    @XmlTransient
    public File getGridwareLogFile() {
        return new File(getWorkDir(), GRIDWARE_LOG);
    }

    @XmlTransient
    public File getStatsFile() {
        return new File(getWorkDir(), RELEASE_STATS_FILE);
    }

    public void setLibraryPath(String libraryPath) {
        this.libraryPath = libraryPath;
    }

    public String getLibraryPath() {
        return libraryPath;
    }

    private static final JAXBContext jaxbc;

    static {
        try {
            jaxbc = JAXBContext.newInstance(FungeneProps.class);
        } catch(JAXBException c) {
            throw new RuntimeException(c);
        }
    }

    public static FungeneProps loadProps(File f) throws IOException {
        return loadProps(new FileInputStream(f));
    }

    public static FungeneProps loadProps(InputStream is) throws IOException {
        try {
            FungeneProps props = (FungeneProps)jaxbc.createUnmarshaller().unmarshal(is);

            if(props.getXmlVersion() != XML_VERSION)
                System.out.println("Property file version (" + props.getXmlVersion() + ") doesn't match expected version (" + XML_VERSION + ")");

            return props;
        } catch(JAXBException e) {
            throw new IOException("Failed to read properties", e);
        }
    }

    public static void writeProps(FungeneProps props, File f) throws JAXBException {
        Marshaller m = jaxbc.createMarshaller();
        m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
        m.marshal(props, f);
    }

    public static void main(String [] args) throws Exception {
        FungeneProps props = new FungeneProps();

        props.setHmmDir("/work/fishjord/other_projects/fungene/hmmer/");
        props.setMotifDir("/work/fishjord/other_projects/fungene/motifs/");
        props.setHmmAlignCmd("hmmalign");
        props.setMinBitsSaved(40);
        props.setBaseDir("/work/fishjord/other_projects/fungene/workdir");
        props.setNrFtpUrl(new URL("ftp://ftp.ncbi.nih.gov/blast/db/FASTA/nr.gz"));
        props.setPfamUrl(new URL("http://pfam.sanger.ac.uk/family/hmm/"));
        props.setLibraryPath("/local/NetBeansProject/AlignmentTools/jni/libhmmerwrapper.dylib");

        props.setReleaseNo(2);

        DataSourceProps dbProps = new DataSourceProps();

        dbProps.setDbUrl("jdbc:postgresql://neptune/fungene");
        dbProps.setUserName("fungene");
        dbProps.setPassword("fungene");

        props.setDbProps(dbProps);

        props.setXmlVersion(XML_VERSION);

        FungeneProps.writeProps(props, new File("default_fg_props.xml"));
    }
}
