package com.yt.sqlparser.test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

public class Question {

    Map<String, Function<String,String>> QuestionMap = new HashMap<>();
    public Question() {
        QuestionMap.put("A", this::handleA);
        QuestionMap.put("B", this::handleB);
        QuestionMap.put("C", this::handleC);
    }
    public String handleQuestion(String question, String answer){
        return Optional.ofNullable(QuestionMap.get(question)).map(f -> f.apply(answer)).orElse(null);
    }
    private String handleA(String answer){
        return String.format("A: %s kobe", answer);
    }
    private String handleB(String answer){
        return  String.format("B: %s james", answer);
    }
    private String handleC(String answer){
        return  String.format("C: %s kyi", answer);
    }
    public static void main(String[] args) {
        Question q = new Question();
        System.out.println(q.handleQuestion("A", "hello"));
        System.out.println(q.handleQuestion("B", "hello"));
        System.out.println(q.handleQuestion("C", "hello"));
    }
}
