package com.kasi.batchpoc.controller;

import com.kasi.batchpoc.model.Asset;
import com.kasi.batchpoc.service.AssetService;
import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping(value={"/asset"})
public class AssestController {
    private AssetService assetService;
    private JobLauncher jobLauncher;
    private Job job;

    public AssestController(AssetService assetService, JobLauncher jobLauncher, Job job) {
        this.assetService = assetService;
        this.jobLauncher = jobLauncher;
        this.job = job;
    }

    @GetMapping(value = "/get", headers = "Accept=application/json")
    public List<Asset> getAllAsset() {
        List<Asset> assets = assetService.list();
        return assets;

    }

    @GetMapping(value = "/load", headers = "Accept=application/json")
    public BatchStatus load() throws JobParametersInvalidException, JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException {


        Map<String, JobParameter> maps = new HashMap<>();
        maps.put("time", new JobParameter(System.currentTimeMillis()));
        JobParameters parameters = new JobParameters(maps);
        JobExecution jobExecution = jobLauncher.run(job, parameters);

        System.out.println("JobExecution: " + jobExecution.getStatus());

        System.out.println("Batch is Running...");
        while (jobExecution.isRunning()) {
            System.out.println("...");
        }

        return jobExecution.getStatus();
    }
}
