package com.tcs.fincore.ReportBuilder.service.facade.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class ReportFileResponse {
    private String fileName;
    private String mediaType;
    private byte[] content;
}