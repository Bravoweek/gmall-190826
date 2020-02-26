package com.mchou.gmallpublisher.service;

import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public interface PublisherService {

    public Integer getDauTotal(String date);

    public Map getDauTotalHourMap(String date);

}
