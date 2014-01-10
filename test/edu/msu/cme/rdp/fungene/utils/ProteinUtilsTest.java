/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package edu.msu.cme.rdp.fungene.utils;

import edu.msu.cme.rdp.readseq.utils.gbregion.RegionParser;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author fishjord
 */
public class ProteinUtilsTest {

    private static final int translTable = 11;
    private static final String testTranslate = "atcatggtgctgccgcgtgacggcctaagggacgacaagggcaaatccatcctctacgaccgcatctattatatcggcgaaaacgacttctacgtccctcgcgacgagcaggggaaattcaagaaatacgaaacggccggcgacaatttcgatgacacgatgaaggtcatgcgcggattgattccgacgcacgtcgtgttcaatggcaaggcgggatcgctgaccggcgagaatgccatgagggcgaaggttggcgagactgttctgatcgtccattcgcaagccaatcgcgatacgcggccgcatctgatcggcggccacggcgattacgtctgggagcaaggcaagttcgccaaccctcccgcaaaggacctggagacctggttcattcgcggcggctcggccggctcggccctctatacattccagcagccgggcatctatgcctacgtgaaccacaacctgatcgaggc";
    private static final String expectedProtein = "mmvlprdglrddkgksilydriyyigendfyvprdeqgkfkkyetagdnfddtmkvmrglipthvvfngkagsltgenamrakvgetvlivhsqanrdtrphligghgdyvweqgkfanppakdletwfirggsagsalytfqqpgiyayvnhnlie";

    public ProteinUtilsTest() {
    }

    /**
     * Test of getTranslationTable method, of class ProteinUtils.
     */
    @Test
    public void testGetTranslationTable() {
        assertEquals(17, ProteinUtils.getInstance().proteinTables());
    }

    /**
     * Test of translateToProtein method, of class ProteinUtils.
     */
    @Test
    public void testTranslateToProtein() {
        assertEquals(expectedProtein, ProteinUtils.getInstance().translateToProtein(testTranslate, false, translTable));
    }

    /**
     * Test of checkBackTranslate method, of class ProteinUtils.
     */
    @Test
    public void testCheckBackTranslate() throws Exception {
        assertEquals(1f, ProteinUtils.getInstance().getTranslScore(expectedProtein, testTranslate, RegionParser.parse("36178456:1..100"), translTable), .0001f);
    }

}