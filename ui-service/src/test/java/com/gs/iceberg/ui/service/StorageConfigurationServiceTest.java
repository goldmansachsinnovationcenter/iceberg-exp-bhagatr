package com.gs.iceberg.ui.service;

import com.gs.iceberg.ui.model.StorageConfiguration;
import com.gs.iceberg.ui.repository.StorageConfigurationRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.core.env.Environment;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for StorageConfigurationService.
 */
public class StorageConfigurationServiceTest {

    @Mock
    private StorageConfigurationRepository storageConfigurationRepository;
    
    @Mock
    private Environment environment;
    
    @InjectMocks
    private StorageConfigurationService storageConfigurationService;
    
    private StorageConfiguration hdfsConfig;
    private StorageConfiguration s3Config;
    private List<StorageConfiguration> configurations;

    @BeforeEach
    public void setup() {
        MockitoAnnotations.openMocks(this);
        
        hdfsConfig = new StorageConfiguration();
        hdfsConfig.setId(1L);
        hdfsConfig.setName("HDFS Configuration");
        hdfsConfig.setStorageType("HDFS");
        hdfsConfig.setBasePath("localhost:9000/iceberg");
        hdfsConfig.setRetentionDays(10);
        hdfsConfig.setKeytabPath("/etc/security/keytabs/hdfs.keytab");
        hdfsConfig.setPrincipal("hdfs@EXAMPLE.COM");
        hdfsConfig.setActive(true);
        hdfsConfig.setCreatedAt(LocalDateTime.now());
        hdfsConfig.setUpdatedAt(LocalDateTime.now());
        
        s3Config = new StorageConfiguration();
        s3Config.setId(2L);
        s3Config.setName("S3 Configuration");
        s3Config.setStorageType("S3");
        s3Config.setBasePath("my-bucket/iceberg");
        s3Config.setRetentionDays(30);
        s3Config.setAccessKey("access-key");
        s3Config.setSecretKey("secret-key");
        s3Config.setRegion("us-east-1");
        s3Config.setActive(false);
        s3Config.setCreatedAt(LocalDateTime.now());
        s3Config.setUpdatedAt(LocalDateTime.now());
        
        configurations = Arrays.asList(hdfsConfig, s3Config);
    }

    @Test
    public void testGetAllConfigurations() {
        when(storageConfigurationRepository.findAll()).thenReturn(configurations);
        
        List<StorageConfiguration> result = storageConfigurationService.getAllConfigurations();
        
        assertEquals(2, result.size(), "Should return two configurations");
        assertEquals("HDFS Configuration", result.get(0).getName(), "First configuration name should match");
        assertEquals("S3 Configuration", result.get(1).getName(), "Second configuration name should match");
        
        verify(storageConfigurationRepository).findAll();
    }

    @Test
    public void testGetConfigurationById_Found() {
        when(storageConfigurationRepository.findById(1L)).thenReturn(Optional.of(hdfsConfig));
        
        Optional<StorageConfiguration> result = storageConfigurationService.getConfigurationById(1L);
        
        assertTrue(result.isPresent(), "Configuration should be found");
        assertEquals("HDFS Configuration", result.get().getName(), "Configuration name should match");
        
        verify(storageConfigurationRepository).findById(1L);
    }

    @Test
    public void testGetConfigurationById_NotFound() {
        when(storageConfigurationRepository.findById(3L)).thenReturn(Optional.empty());
        
        Optional<StorageConfiguration> result = storageConfigurationService.getConfigurationById(3L);
        
        assertFalse(result.isPresent(), "Configuration should not be found");
        
        verify(storageConfigurationRepository).findById(3L);
    }

    @Test
    public void testGetConfigurationByName_Found() {
        when(storageConfigurationRepository.findByName("HDFS Configuration")).thenReturn(Optional.of(hdfsConfig));
        
        Optional<StorageConfiguration> result = storageConfigurationService.getConfigurationByName("HDFS Configuration");
        
        assertTrue(result.isPresent(), "Configuration should be found");
        assertEquals("HDFS", result.get().getStorageType(), "Storage type should match");
        
        verify(storageConfigurationRepository).findByName("HDFS Configuration");
    }

    @Test
    public void testGetActiveConfiguration_Found() {
        when(storageConfigurationRepository.findByActiveTrue()).thenReturn(Optional.of(hdfsConfig));
        
        Optional<StorageConfiguration> result = storageConfigurationService.getActiveConfiguration();
        
        assertTrue(result.isPresent(), "Active configuration should be found");
        assertEquals("HDFS Configuration", result.get().getName(), "Active configuration name should match");
        
        verify(storageConfigurationRepository).findByActiveTrue();
    }

    @Test
    public void testCreateConfiguration() {
        when(storageConfigurationRepository.save(any(StorageConfiguration.class))).thenReturn(s3Config);
        
        StorageConfiguration result = storageConfigurationService.createConfiguration(s3Config);
        
        assertNotNull(result, "Created configuration should not be null");
        assertEquals("S3 Configuration", result.getName(), "Configuration name should match");
        
        verify(storageConfigurationRepository).save(any(StorageConfiguration.class));
    }

    @Test
    public void testUpdateConfiguration_Found() {
        StorageConfiguration updatedConfig = new StorageConfiguration();
        updatedConfig.setName("Updated S3 Configuration");
        updatedConfig.setStorageType("S3");
        updatedConfig.setBasePath("updated-bucket/iceberg");
        updatedConfig.setRetentionDays(60);
        updatedConfig.setAccessKey("updated-access-key");
        updatedConfig.setSecretKey("updated-secret-key");
        updatedConfig.setRegion("us-west-2");
        updatedConfig.setActive(true);
        
        when(storageConfigurationRepository.findById(2L)).thenReturn(Optional.of(s3Config));
        when(storageConfigurationRepository.save(any(StorageConfiguration.class))).thenAnswer(invocation -> invocation.getArgument(0));
        
        Optional<StorageConfiguration> result = storageConfigurationService.updateConfiguration(2L, updatedConfig);
        
        assertTrue(result.isPresent(), "Updated configuration should be present");
        assertEquals("Updated S3 Configuration", result.get().getName(), "Configuration name should be updated");
        assertEquals("updated-bucket/iceberg", result.get().getBasePath(), "Base path should be updated");
        assertEquals(60, result.get().getRetentionDays(), "Retention days should be updated");
        assertEquals("updated-access-key", result.get().getAccessKey(), "Access key should be updated");
        assertEquals("updated-secret-key", result.get().getSecretKey(), "Secret key should be updated");
        assertEquals("us-west-2", result.get().getRegion(), "Region should be updated");
        assertTrue(result.get().isActive(), "Active flag should be updated");
        
        verify(storageConfigurationRepository).findById(2L);
        verify(storageConfigurationRepository).save(any(StorageConfiguration.class));
    }

    @Test
    public void testDeactivateAllConfigurations() {
        when(storageConfigurationRepository.findAll()).thenReturn(configurations);
        
        storageConfigurationService.deactivateAllConfigurations();
        
        verify(storageConfigurationRepository).findAll();
        verify(storageConfigurationRepository).save(hdfsConfig);
        verify(storageConfigurationRepository, never()).save(s3Config);
        
        assertFalse(hdfsConfig.isActive(), "HDFS configuration should be deactivated");
    }

    @Test
    public void testActivateConfiguration() {
        when(storageConfigurationRepository.findAll()).thenReturn(configurations);
        when(storageConfigurationRepository.findById(2L)).thenReturn(Optional.of(s3Config));
        when(storageConfigurationRepository.save(any(StorageConfiguration.class))).thenAnswer(invocation -> invocation.getArgument(0));
        
        Optional<StorageConfiguration> result = storageConfigurationService.activateConfiguration(2L);
        
        assertTrue(result.isPresent(), "Activated configuration should be present");
        assertTrue(result.get().isActive(), "S3 configuration should be activated");
        
        verify(storageConfigurationRepository).findAll();
        verify(storageConfigurationRepository).save(hdfsConfig);
        verify(storageConfigurationRepository).findById(2L);
        verify(storageConfigurationRepository).save(s3Config);
    }

    @Test
    public void testApplyActiveConfiguration_HDFS() {
        when(storageConfigurationRepository.findByActiveTrue()).thenReturn(Optional.of(hdfsConfig));
        
        boolean result = storageConfigurationService.applyActiveConfiguration();
        
        assertTrue(result, "Configuration should be applied successfully");
        
        verify(storageConfigurationRepository).findByActiveTrue();
    }

    @Test
    public void testApplyActiveConfiguration_S3() {
        s3Config.setActive(true);
        
        when(storageConfigurationRepository.findByActiveTrue()).thenReturn(Optional.of(s3Config));
        
        boolean result = storageConfigurationService.applyActiveConfiguration();
        
        assertTrue(result, "Configuration should be applied successfully");
        
        verify(storageConfigurationRepository).findByActiveTrue();
    }

    @Test
    public void testApplyActiveConfiguration_NoActiveConfig() {
        when(storageConfigurationRepository.findByActiveTrue()).thenReturn(Optional.empty());
        
        boolean result = storageConfigurationService.applyActiveConfiguration();
        
        assertFalse(result, "Configuration should not be applied");
        
        verify(storageConfigurationRepository).findByActiveTrue();
    }

    @Test
    public void testInitializeDefaultConfiguration_Empty() {
        when(storageConfigurationRepository.count()).thenReturn(0L);
        
        storageConfigurationService.initializeDefaultConfiguration();
        
        verify(storageConfigurationRepository).count();
        verify(storageConfigurationRepository).save(any(StorageConfiguration.class));
    }

    @Test
    public void testInitializeDefaultConfiguration_NotEmpty() {
        when(storageConfigurationRepository.count()).thenReturn(2L);
        
        storageConfigurationService.initializeDefaultConfiguration();
        
        verify(storageConfigurationRepository).count();
        verify(storageConfigurationRepository, never()).save(any(StorageConfiguration.class));
    }
}
