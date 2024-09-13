package com.atguigu.online.education.dws.util;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class KeywordUtil {
    public static List<String> analyze(String text){
        List<String> keywordList = new ArrayList<>();
        StringReader reader = new StringReader(text);
        IKSegmenter ik = new IKSegmenter(reader, true);
        try {
            Lexeme lexeme = null;
            while ((lexeme = ik.next()) != null){
                String keyword = lexeme.getLexemeText();
                keywordList.add(keyword);
            }
        } catch (Exception e){
            throw new RuntimeException(e);
        }
        return keywordList;
    }
}
