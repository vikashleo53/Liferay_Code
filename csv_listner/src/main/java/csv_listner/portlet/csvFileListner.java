package csv_listner.portlet;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;

import com.liferay.document.library.kernel.model.DLFileEntry;
import com.liferay.document.library.kernel.service.DLAppServiceUtil;
import com.liferay.object.model.ObjectDefinition;
import com.liferay.object.model.ObjectEntry;
import com.liferay.object.service.ObjectDefinitionLocalService;
import com.liferay.object.service.ObjectEntryLocalService;
import com.liferay.portal.kernel.exception.ModelListenerException;
import com.liferay.portal.kernel.json.JSONFactoryUtil;
import com.liferay.portal.kernel.json.JSONObject;
import com.liferay.portal.kernel.log.Log;
import com.liferay.portal.kernel.log.LogFactoryUtil;
import com.liferay.portal.kernel.model.BaseModelListener;
import com.liferay.portal.kernel.model.ModelListener;
import com.liferay.portal.kernel.repository.model.FileEntry;
import com.liferay.portal.kernel.repository.model.FileVersion;
import com.liferay.portal.kernel.service.ServiceContext;

@Component(immediate = true, service = ModelListener.class)
public class csvFileListner extends BaseModelListener<DLFileEntry> {

    private static final Log _log = LogFactoryUtil.getLog(csvFileListner.class);

    @Reference
    private ObjectEntryLocalService _objectEntryLocalService;

    @Reference
    private ObjectDefinitionLocalService _objectDefinitionLocalService;

    @Override
    public void onAfterUpdate(DLFileEntry originalModel, DLFileEntry fileEntry) throws ModelListenerException {
        String fileName = fileEntry.getFileName();
        if (fileName.endsWith(".csv")) {
            try {
                long fileEntryId = fileEntry.getFileEntryId();
                _log.info("CSV file detected: " + fileName + " | fileEntryId=" + fileEntryId);

                // Process the CSV file
                readAndSaveToObject(fileEntryId, fileEntry.getGroupId(), fileEntry.getUserId(), fileEntry.getCompanyId());
            } catch (Exception e) {
                throw new ModelListenerException(e);
            }
        }
    }

    private void readAndSaveToObject(long fileEntryId, long groupId, long userId, long companyId) {
        try {
            FileEntry fileEntry = DLAppServiceUtil.getFileEntry(fileEntryId);

            // Always get the latest version instead of assuming "1.0"
            FileVersion fileVersion = fileEntry.getLatestFileVersion();
            InputStream inputStream = fileVersion.getContentStream(false);
            List<FileVersion> versions = fileEntry.getFileVersions(0);
            for (FileVersion version : versions) {
                _log.info("Available version: " + version.getVersion() + " | status=" + version.getStatus());
            }


            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

            String headerLine = reader.readLine(); // header
            if (headerLine == null) {
                _log.warn("Empty CSV file.");
                return;
            }

            // Build map of headers
            String[] headers = headerLine.split(",");
            Map<String, Integer> columnIndexMap = new HashMap<>();
            for (int i = 0; i < headers.length; i++) {
                columnIndexMap.put(headers[i].trim(), i);
            }

            String line;
            while ((line = reader.readLine()) != null) {
                String[] values = line.split(",");

                String studentName = getValue(values, columnIndexMap, "studentName");
                String rollNumber = getValue(values, columnIndexMap, "rollNumber");

                // Build JSON object for fields
                JSONObject jsonObject = JSONFactoryUtil.createJSONObject();
                jsonObject.put("studentName", studentName);
                jsonObject.put("rollNumber", rollNumber);

                // Get ObjectDefinition for "Student"
                ObjectDefinition objectDefinition =
                        _objectDefinitionLocalService.fetchObjectDefinitionByExternalReferenceCode("student", companyId);
                if (objectDefinition == null) {
                    _log.error("Object definition 'Student' not found.");
                    continue;
                }

                ServiceContext serviceContext = new ServiceContext();
                serviceContext.setScopeGroupId(0);

                Map<String, Serializable> values1 = new HashMap<>();
                values1.put("studentName", studentName);
                values1.put("rollNumber", rollNumber);

                ObjectEntry objectEntry = _objectEntryLocalService.addObjectEntry( userId,0,objectDefinition.getObjectDefinitionId(), values1,serviceContext);
                       
                _log.info("ObjectEntry created with ID: " + objectEntry.getObjectEntryId());
                _log.info("Added ObjectEntry for student: " + studentName + ", roll: " + rollNumber);
            }

            reader.close();
        } catch (Exception e) {
            _log.error("Error processing CSV", e);
        }
    }

    private String getValue(String[] values, Map<String, Integer> columnIndexMap, String column) {
        Integer index = columnIndexMap.get(column);
        if (index != null && index < values.length) {
            return values[index].trim();
        }
        return "";
    }
}