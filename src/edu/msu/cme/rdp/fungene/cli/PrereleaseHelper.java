/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.msu.cme.rdp.fungene.cli;

import edu.msu.cme.rdp.fungene.db.FungeneDB;
import edu.msu.cme.rdp.fungene.utils.FungeneProps;
import java.io.File;
import java.io.IOException;
import java.sql.*;
import javax.xml.bind.JAXBException;
import org.apache.commons.io.FileUtils;

/**
 *
 * @author fishjord
 */
public class PrereleaseHelper extends FungeneDB {

    public PrereleaseHelper(FungeneProps props) {
        super(props);
    }


    public int createRelease(String label, String releaseDesc) {
        Connection dbConn = null;
        PreparedStatement prepStmt = null;
        Statement stmt = null;
        ResultSet rset = null;

        try {
            dbConn = ds.getConnection();

            dbConn.setAutoCommit(false);

            stmt = dbConn.createStatement();

            rset = stmt.executeQuery("select nextval('release_id_seq')");
            rset.next();
            int ret = rset.getInt(1);
            rset.close();

            prepStmt = dbConn.prepareStatement("insert into release values(?, ?, now(), ?)");
            prepStmt.setInt(1, ret);
            prepStmt.setString(2, label);
            if (releaseDesc == null) {
                prepStmt.setNull(3, Types.VARCHAR);
            } else {
                prepStmt.setString(3, releaseDesc);
            }

            prepStmt.execute();

            dbConn.commit();

            return ret;
        } catch (SQLException e) {
            try {
                dbConn.rollback();
            } catch (Exception ee) {
            }
            throw new RuntimeException(e);
        } finally {
            try {
                rset.close();
            } catch (Exception e) {
            }
            try {
                stmt.close();
            } catch (Exception e) {
            }
            try {
                prepStmt.close();
            } catch (Exception e) {
            }
            try {
                dbConn.close();
            } catch (Exception e) {
            }
        }
    }

    public static File prereleaseSetup(FungeneProps skelProps, String releaseName, String releaseDescription) throws JAXBException, IOException {
        PrereleaseHelper loader = new PrereleaseHelper(skelProps);

        int nextRelease = loader.createRelease(releaseName, releaseDescription);
        skelProps.setReleaseNo(nextRelease);

        if (skelProps.getWorkDir().exists()) {
            System.out.print("Release directory '" + skelProps.getWorkDir() + "' exists, nuking in ");
            for (int index = 5; index >= 0; index--) {
                System.out.print(index + "..");

                try {
                    Thread.sleep(1000);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            System.out.println();

            FileUtils.forceDelete(skelProps.getWorkDir());
        }

        skelProps.getWorkDir().mkdir();

        File ret = new File(skelProps.getWorkDir(), "release_" + nextRelease + ".xml");
        FungeneProps.writeProps(skelProps, ret);
        return ret;
    }
}
