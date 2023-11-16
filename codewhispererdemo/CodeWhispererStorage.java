package com.amazon.aws.vector.consolas.controlplane.service.codewhispererdemo;

import com.amazon.aws.vector.consolas.commons.exception.DependencyException;
import com.amazon.aws.vector.consolas.controlplane.service.codewhispererdemo.exception.CodeWhispererPictureException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.util.concurrent.TimeUnit;
import java.util.UUID;

public class CodeWhispererStorage {

    private static final Logger LOGGER = LoggerFactory.getLogger(CodeWhispererStorage.class);
    private static CodeWhispererStorage singletonInstance = null;
    private final S3AsyncClient s3Client;
    private final String BUCKET_NAME = "CodeWhispererPictures";

    public static CodeWhispererStorage getInstance() {
        if (singletonInstance == null) {
            singletonInstance = new CodeWhispererStorage(S3AsyncClient.builder().build());
        }
        return singletonInstance;
    }

    private CodeWhispererStorage(final S3AsyncClient s3Client) {
        this.s3Client = s3Client;
    }

    public String storePictureToStorage(final String pictureId, final RequestBody requestBody, final int timeout) {
        final PutObjectRequest objectRequest = PutObjectRequest.builder()
                .bucket(BUCKET_NAME)
                .key(pictureId)
                .build();
        try {
            s3Client.putObject(objectRequest, requestBody).get(timeout, TimeUnit.SECONDS);
            return UUID.randomUUID() + "-" + pictureId;
        } catch (Exception e) {
            String message = String.format("Exception occurred while storing metadata for pictureId: %s", pictureId);
            LOGGER.error(message, e);
            throw new CodeWhispererPictureException(message, e);
        }
    }

    public boolean doesObjectExist(final String pictureId) {
        HeadObjectRequest headObjectRequest = HeadObjectRequest.builder()
                .bucket(BUCKET_NAME)
                .key(pictureId)
                .build();

        try {
            this.s3Client.headObject(headObjectRequest).get();
        } catch (NoSuchKeyException e) {
            return false;
        } catch (Exception e) {
            String message = String.format("Exception occurred while getting metadata for pictureId: %s", pictureId);
            LOGGER.error(message, e);
            throw new CodeWhispererPictureException(message, e);
        }
        return true;
    }

    public void deletePictureFromStorage(final String pictureId, final int timeout) {
        DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder()
                .bucket(BUCKET_NAME)
                .key(pictureId)
                .build();

        try {
            this.s3Client.deleteObject(deleteObjectRequest).get(timeout, TimeUnit.SECONDS);
        } catch (NoSuchKeyException e) {
            // already deleted
            LOGGER.info("Already deleted");
        } catch (Exception e) {
            String message = String.format("Exception occurred while deleting metadata for pictureId: %s", pictureId);
            LOGGER.error(message, e);
            throw new CodeWhispererPictureException(message, e);
        }
    }
}
