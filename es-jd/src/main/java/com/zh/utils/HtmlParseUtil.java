package com.zh.utils;

import com.zh.pojo.Content;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;

@Component
public class HtmlParseUtil {

//    public static void main(String[] args) throws IOException {
//
//        new HtmlParseUtil().parseJD("vue").forEach(System.out::println);
//
//    }

    public ArrayList<Content> parseJD(String keywords) throws IOException {
        // 获取请求   https://search.jd.com/Search?keyword=java
        String url = "https://search.jd.com/Search?keyword="+keywords;

        // 解析网页   返回的Document就是JavaScript的Document对象
        Document document = Jsoup.parse(new URL(url), 30000);

        // 所有在js中可以使用的方法，这里都可以使用
        Element element = document.getElementById("J_goodsList");

        // 获取所有的li标签
        Elements elements = element.getElementsByTag("li");

        ArrayList<Content> list = new ArrayList<>();

        // 获取元素的内容 el 是每一个li标签
        for (Element el : elements) {
            String img = el.getElementsByTag("img").eq(0).attr("src");
            String price = el.getElementsByClass("p-price").eq(0).text();
            String title = el.getElementsByClass("p-name").eq(0).text();

            Content content = new Content(title, img, price);

            list.add(content);
        }

        return list;
    }

}
