package com.mikerusoft.cdc.serdes.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class SectionData {
    private Integer sectionId;
    private Long dyid;
    private Long rri;
    private Long timestamp;
}
