package com.neu.edu.assign6part4;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

public class MovieTagReducer extends Reducer<Text, Text, Text, NullWritable> {

	private ArrayList<String> tags = new ArrayList<String>();
	private DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
	private String movieTitle = null;

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		movieTitle = null;
		tags.clear();

		for (Text val : values) {
			if (val.charAt(0) == 'M') {
				movieTitle = val.toString().substring(1, val.toString().length()).trim();
			} else {
				tags.add(val.toString().substring(1, val.toString().length()).trim());
			}
		}

		if (movieTitle != null) {
			try {
				String movieTitleWithTags = nestElements(movieTitle, tags);

				context.write(new Text(movieTitleWithTags), NullWritable.get());
			} catch (TransformerException ex) {
				Logger.getLogger(MovieTagReducer.class.getName()).log(Level.SEVERE, null, ex);
			} catch (ParserConfigurationException ex) {
				Logger.getLogger(MovieTagReducer.class.getName()).log(Level.SEVERE, null, ex);
			} catch (SAXException ex) {
				Logger.getLogger(MovieTagReducer.class.getName()).log(Level.SEVERE, null, ex);
			}
		}

	}

	private String nestElements(String movieTitle, List<String> tags)
			throws TransformerException, ParserConfigurationException, SAXException, IOException {
		DocumentBuilder bldr = dbf.newDocumentBuilder();
		Document doc = bldr.newDocument();
		String[] title = movieTitle.split(",");
		String movieTitleEl = title[1];
		Element toAddMovieTitleEl = doc.createElement("movieTitle");
		copyAttributesToElement("movieTitle", movieTitleEl, toAddMovieTitleEl);

		for (String tagsXml : tags) {
			String[] tag = tagsXml.split(",");
			String tagsEl = tag[2];
			Element toAddtagsEl = doc.createElement("tags");
			copyAttributesToElement("tags", tagsEl, toAddtagsEl);
			toAddMovieTitleEl.appendChild(toAddtagsEl);
		}
		doc.appendChild(toAddMovieTitleEl);
		return transformDocumentToString(doc);
	}

	private void copyAttributesToElement(String key, String value, Element element) {
		element.setAttribute(key, value);
	}

	private String transformDocumentToString(Document doc)
			throws TransformerConfigurationException, TransformerException {
		TransformerFactory tf = TransformerFactory.newInstance();
		Transformer transformer = tf.newTransformer();
		transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
		StringWriter writer = new StringWriter();
		transformer.transform(new DOMSource(doc), new StreamResult(writer));
		return writer.getBuffer().toString().replaceAll("\n|\r", "");
	}
}
