package com.mikerusoft.cdc.serdes.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class SectionKey {
    private Integer sectionId;
    private Long dyid;
    private Long rri;

    public static SectionKey buildFrom(SectionData data) {
        return new SectionKey(data.getSectionId(), data.getDyid(), data.getRri());
    }
}
