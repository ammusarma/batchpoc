package com.kasi.batchpoc.model;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;

@Entity(name="ASSET")
public class Asset {
    public Asset(){}

    public Asset(int assetId, String assetTypeCode) {
        this.assetId = assetId;
        this.assetTypeCode = assetTypeCode;
    }

    public int getAssetId() {
        return assetId;
    }

    public void setAssetId(int assetId) {
        this.assetId = assetId;
    }

    public String getAssetTypeCode() {
        return assetTypeCode;
    }

    public void setAssetTypeCode(String assetTypeCode) {
        this.assetTypeCode = assetTypeCode;
    }

    @Id
    @Column(name="ASSET_ID")
    private int assetId;
    @Column(name="ASSET_TYPE_CD")
    private String assetTypeCode;
}
