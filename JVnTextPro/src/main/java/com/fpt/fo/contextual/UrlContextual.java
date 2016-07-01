package com.fpt.fo.contextual;

import com.fpt.fo.crawler.CrawlerServiceImpl;

public class UrlContextual {
	private IntegratedContextProcess integratedContextProcess;
	private CrawlerServiceImpl crawlerServiceImpl;

	public UrlContextual() {
		integratedContextProcess = IntegratedContextProcess.getInstance();
		crawlerServiceImpl = CrawlerServiceImpl.getInstance();
	}

	public static void main(String[] args) {
		UrlContextual urlContextual = new UrlContextual();
		String url = "http://wit-ecogreen.com.vn/wit-cong-thu-cong-dung.html?utm_source=VnExpress&utm_medium=statis&utm_campaign=C111243";
		String content = urlContextual.getUrlContent(url);
		String text = "Ông Phạm Thanh Hải, sinh năm 1966. Theo website của IDT – nơi ông giữ chức vụ Chủ tịch Hội đồng quản trị kiêm Tổng giám đốc, ông Hải bảo vệ Tiến sỹ tại trường Đại học Tổng hợp Belarus (Liên Xô cũ) từ năm 1994. Cũng theo đó, ông có nhiều năm kinh nghiệm hoạt động và làm việc tại Trung tâm Thông tin khoa học kỹ thuật quốc tế (Nga).";
		urlContextual.test(text);
	}
	
	public void test(String text){
		System.out.println("\n---MAXENT---");
		System.out.println(integratedContextProcess.getPOSTaggerMaxent(text));
//		System.out.println("\n---CRFs---");
//		System.out.println(integratedContextProcess.getPOSTaggerCRF(text));
	}

	public String getUrlContent(String url) {
		return crawlerServiceImpl.crawlerUrl(url, true, false);
	}

	public String getUrlContentSimple(String url) {
		return crawlerServiceImpl.crawlerUrlSimple(url);
	}

}
