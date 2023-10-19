package io.tapdata.connector.redis.bean;

public class ZSetElement {

    private String element;
    private double score;

    public String getElement() {
        return element;
    }

    public void setElement(String element) {
        this.element = element;
    }

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }
}
