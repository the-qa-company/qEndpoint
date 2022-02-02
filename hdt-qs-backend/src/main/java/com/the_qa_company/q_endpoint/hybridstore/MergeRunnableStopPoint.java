package com.the_qa_company.q_endpoint.hybridstore;

/**
 * only for test purpose
 */
public enum MergeRunnableStopPoint {
    STEP1_START("at start of step1/merge"),
    STEP1_END("before step1 marker write"),
    STEP2_START("at start of step2"),
    STEP2_END("before step2 marker write"),
    STEP3_START("at start of step3"),
    STEP3_END("before step3 marker write"),
    MERGE_END("after merge file deletion"),
    MERGE_END_AFTER_SLEEP("after MERGE_END and sleep");

    public class MergeRunnableStopException extends RuntimeException {
        public MergeRunnableStopException() {
            super("crashing merge at point: "+ name().toLowerCase() + " (" + description + ")");
        }

        public MergeRunnableStopPoint getStopPoint() {
            return MergeRunnableStopPoint.this;
        }
    }

    private String description;

    MergeRunnableStopPoint(String desc) {
        this.description = desc;
    }

    public void throwStop() {
        throw new MergeRunnableStopException();
    }

    public String getDescription() {
        return description;
    }
}
