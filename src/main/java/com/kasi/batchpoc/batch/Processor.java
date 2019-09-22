package com.kasi.batchpoc.batch;

import com.kasi.batchpoc.model.Asset;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

@Component("CsvToH2_Processor")
public class Processor implements ItemProcessor<Asset, Asset> {
    public Processor() {
    }

    @Override
    public Asset process(Asset asset) throws Exception {
        return asset;
    }
}