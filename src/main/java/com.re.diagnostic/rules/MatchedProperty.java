package com.re.diagnostic.rules;

import lombok.Data;

@Data
public class MatchedProperty {

    private final Long dtcId;
    private final String dtcCode;
    private final String description;
    private final boolean matched;
    private final String severity;
    private final Integer version;

}
