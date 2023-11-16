package com.amazon.aws.vector.consolas.controlplane.service.codewhispererdemo;

import com.amazon.aws.vector.consolas.controlplane.service.codewhispererdemo.exception.CodeWhispererPictureException;
import com.amazon.aws.vector.consolas.controlplane.service.codewhispererdemo.model.CodeWhispererPictureMetadata;
import com.amazon.aws.vector.consolas.controlplane.service.codewhispererdemo.model.ImmutableCodeWhispererPictureMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;

import javax.inject.Inject;
import java.util.Optional;

public class CodeWhispererPictureSaveActivity {
    private static final Logger LOGGER = LoggerFactory.getLogger(CodeWhispererPictureSaveActivity.class);

    /**
     * Persists the CodeWhisperer picture in the system.
     */
    public String savePicture(final String pictureId, final RequestBody picture) {
        Optional<CodeWhispererPictureMetadata> pictureMetadataFromCache = CodeWhispererCache.getInstance().get(pictureId);
        if (pictureMetadataFromCache.isPresent()) {
            String message = String.format("Picture already exists for pictureId %s", pictureId);
            LOGGER.error(message);
            throw new CodeWhispererPictureException(message);
        }
        // attempt to store picture for at most 10 seconds.
        int timeout = 10;
        String storageLocation = CodeWhispererStorage.getInstance().storePictureToStorage(pictureId, picture, timeout);

        CodeWhispererPictureMetadata pictureMetadata = ImmutableCodeWhispererPictureMetadata.builder()
                .withPictureId(pictureId)
                .withName(pictureId)
                .withStorageLocation(storageLocation)
                .build();
        CodeWhispererCache.getInstance().put(pictureId, pictureMetadata);

        CodeWhispererDB.getInstance().storePictureMetadata(pictureMetadata);
        return pictureId;
    }

    /**
     * Gets the CodeWhisperer picture location.
     */
    public String getPictureLocation(final String pictureId) {
        Optional<CodeWhispererPictureMetadata> pictureMetadataFromCache = CodeWhispererCache.getInstance().get(pictureId);
        if (pictureMetadataFromCache.isPresent()) {
            return pictureMetadataFromCache.get().getStorageLocation();
        } else {
            return CodeWhispererDB.getInstance().getPictureMetadata(pictureId).getStorageLocation();
        }
    }
}
