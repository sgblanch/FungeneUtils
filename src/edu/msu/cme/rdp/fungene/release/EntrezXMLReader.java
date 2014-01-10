/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.msu.cme.rdp.fungene.release;

import com.sun.org.apache.xerces.internal.impl.PropertyManager;
import com.sun.org.apache.xerces.internal.impl.XMLStreamReaderImpl;
import edu.msu.cme.rdp.eutils.EUtilsSeqHolder;
import gov.nih.nlm.ncbi.www.soap.eutils.EFetchSequenceServiceStub;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import javax.xml.namespace.QName;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

/**
 *
 * @author fishjord
 */
public class EntrezXMLReader {

    private static class XMLStreamReaderHack extends XMLStreamReaderImpl {

        private static final String namespace = "http://www.ncbi.nlm.nih.gov/soap/eutils/efetch_seq";

        public XMLStreamReaderHack(Reader r) throws XMLStreamException {
            super(r, new PropertyManager(PropertyManager.CONTEXT_READER));
        }

        @Override
        public QName getName() {
            return new QName(namespace, this.getLocalName());
        }
    }
    private XMLStreamReader xmlReader;
    private boolean more = true;

    public EntrezXMLReader(Reader in) throws XMLStreamException {
        if (in instanceof BufferedReader) {
            xmlReader = new XMLStreamReaderHack(in);
        } else {
            xmlReader = new XMLStreamReaderHack(new BufferedReader(in));
        }
    }
    public EntrezXMLReader(InputStream in) throws XMLStreamException {
        this(new InputStreamReader(in));
    }

    public EUtilsSeqHolder readNext() throws Exception {
        if(!more) {
            return null;
        }
        int event;

        while ((event = xmlReader.next()) != XMLStreamConstants.END_DOCUMENT) {
            if (event != XMLStreamConstants.START_ELEMENT) {
                continue;
            }

            if("INSDSeq".equals(xmlReader.getLocalName())) {
                return new EUtilsSeqHolder(EFetchSequenceServiceStub.INSDSeq_type0.Factory.parse(xmlReader));
            } else if("GBSeq".equals(xmlReader.getLocalName())) {
                return new EUtilsSeqHolder(EFetchSequenceServiceStub.GBSeq_type0.Factory.parse(xmlReader));
            }
        }

        more = false;
        return null;
    }

    public void close() throws XMLStreamException {
        xmlReader.close();
    }
}
