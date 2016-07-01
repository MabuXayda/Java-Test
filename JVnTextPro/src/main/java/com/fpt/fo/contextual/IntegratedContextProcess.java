package com.fpt.fo.contextual;

import java.io.File;

import jvntextpro.JVnTextPro;

public class IntegratedContextProcess {

	private final String POS_MAXENT = "maxent";
	private final String POS_CRF = "crfs";
	private JVnTextPro jVnTextPro;
	private static IntegratedContextProcess instance;

	private IntegratedContextProcess() {
		jVnTextPro = new JVnTextPro();
		System.out.println();
		jVnTextPro.initSenSegmenter("./models/jvnsensegmenter");
		System.out.println();
		jVnTextPro.initSegmenter("./models/jvnsegmenter");
		System.out.println();
		jVnTextPro.initPosTaggerMaxent("./models/jvnpostag" + File.separator + POS_MAXENT);
		System.out.println();
		jVnTextPro.initPosTaggerCRF("./models/jvnpostag" + File.separator + POS_CRF);
	}

	public static IntegratedContextProcess getInstance() {
		if (instance == null) {
			synchronized (IntegratedContextProcess.class) {
				if (instance == null) {
					instance = new IntegratedContextProcess();
				}
			}
		}
		return instance;
	}

	public String getSentences(String text) {
		if (text == null || text.isEmpty()) {
			return "";
		}
		String ret = text;

		// Pipeline
		ret = jVnTextPro.convert(ret);
		ret = jVnTextPro.senSegment(ret);
		ret = jVnTextPro.senTokenize(ret);
		return ret;
	}

	public String getWords(String text) {
		if (text == null || text.isEmpty()) {
			return "";
		}
		String ret = getSentences(text);
		// Pipeline
//		ret = jVnTextPro.postProcessing(ret);
		ret = jVnTextPro.wordSegment(ret);
		return ret;
	}

	public String getPOSTaggerMaxent(String text) {
		if (text == null || text.isEmpty()) {
			return "";
		}
		String ret = getWords(text);
		// Pipeline
//		ret = jVnTextPro.posTaggingMaxent(ret);
		return ret;
	}
	
	public String getPOSTaggerCRF(String text) {
		if (text == null || text.isEmpty()) {
			return "";
		}
		String ret = getWords(text);
		// Pipeline
		ret = jVnTextPro.posTaggingCRF(ret);
		return ret;
	}
	
	public static void main(String[] args) {
		IntegratedContextProcess integratedContextProcess = new IntegratedContextProcess();
	}
	

}
