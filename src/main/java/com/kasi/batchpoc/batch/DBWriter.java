package com.kasi.batchpoc.batch;

import com.kasi.batchpoc.model.Asset;
import com.kasi.batchpoc.repository.AssetRepository;
import org.springframework.batch.item.ItemWriter;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class DBWriter implements ItemWriter<Asset> {

    private AssetRepository assetRepository;

    public DBWriter(AssetRepository assetRepository) {
        this.assetRepository = assetRepository;
    }

    @Override
    public void write(List<? extends Asset> assets) throws Exception {
        assetRepository.saveAll(assets);
    }
}
