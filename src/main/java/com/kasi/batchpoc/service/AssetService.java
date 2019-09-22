package com.kasi.batchpoc.service;

import com.kasi.batchpoc.model.Asset;
import com.kasi.batchpoc.repository.AssetRepository;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class AssetService {
    private AssetRepository assetRepository;

    public AssetService(AssetRepository assetRepository) {
        this.assetRepository = assetRepository;
    }

    public List<Asset> list() {
        return assetRepository.findAll();
    }
}
