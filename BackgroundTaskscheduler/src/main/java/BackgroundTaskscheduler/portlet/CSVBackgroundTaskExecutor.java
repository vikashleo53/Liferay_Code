package BackgroundTaskscheduler.portlet;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;

import com.liferay.document.library.kernel.service.DLAppServiceUtil;
import com.liferay.object.model.ObjectDefinition;
import com.liferay.object.model.ObjectEntry;
import com.liferay.object.service.ObjectDefinitionLocalService;
import com.liferay.object.service.ObjectEntryLocalService;
import com.liferay.portal.kernel.backgroundtask.BackgroundTask;
import com.liferay.portal.kernel.backgroundtask.BackgroundTaskExecutor;
import com.liferay.portal.kernel.backgroundtask.BackgroundTaskResult;
import com.liferay.portal.kernel.backgroundtask.BaseBackgroundTaskExecutor;
import com.liferay.portal.kernel.backgroundtask.display.BackgroundTaskDisplay;
import com.liferay.portal.kernel.log.Log;
import com.liferay.portal.kernel.log.LogFactoryUtil;
import com.liferay.portal.kernel.repository.model.FileEntry;
import com.liferay.portal.kernel.repository.model.FileVersion;
import com.liferay.portal.kernel.service.ServiceContext;
import com.liferay.portal.kernel.util.MapUtil;
@Component(
	    immediate = true,
	    property = {
	            "background.task.executor.class.name=BackgroundTaskscheduler.portlet.CSVBackgroundTaskExecutor"
	    },
	    service = BackgroundTaskExecutor.class
	)
public class CSVBackgroundTaskExecutor extends BaseBackgroundTaskExecutor{
	
	  private static final Log _log = LogFactoryUtil.getLog(CSVBackgroundTaskExecutor.class);
	  
	@Override
	public BackgroundTaskResult execute(BackgroundTask backgroundTask) throws Exception {
	      Map<String, Serializable> taskContextMap = backgroundTask.getTaskContextMap();

	        long fileEntryId = MapUtil.getLong(taskContextMap, "fileEntryId");
	        long groupId = MapUtil.getLong(taskContextMap, "groupId");
	        long userId = MapUtil.getLong(taskContextMap, "userId");
	        long companyId = MapUtil.getLong(taskContextMap, "companyId");

	        _log.info("Running CSV import in background for FileEntry ID: " + fileEntryId);

	        FileEntry fileEntry = DLAppServiceUtil.getFileEntry(fileEntryId);
	        FileVersion fileVersion = fileEntry.getLatestFileVersion();
	        InputStream inputStream = fileVersion.getContentStream(false);
	        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

	        String headerLine = reader.readLine();
	        if (headerLine == null) {
	            _log.warn("CSV file is empty.");
	            return BackgroundTaskResult.SUCCESS;
	        }

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

	            ObjectDefinition objectDefinition =
	                    _objectDefinitionLocalService.fetchObjectDefinitionByExternalReferenceCode("student", companyId);

	            if (objectDefinition == null) {
	                _log.error("Object definition 'student' not found.");
	                continue;
	            }

	            Map<String, Serializable> fields = new HashMap<>();
	            fields.put("studentName", studentName);
	            fields.put("rollNumber", rollNumber);

	            ServiceContext serviceContext = new ServiceContext();
	            serviceContext.setScopeGroupId(groupId);

	            ObjectEntry entry = _objectEntryLocalService.addObjectEntry(
	                    userId, 0, objectDefinition.getObjectDefinitionId(), fields, serviceContext);

	            _log.info("Created ObjectEntry for: " + studentName + " - " + rollNumber);
	        }

	        reader.close();

	        return BackgroundTaskResult.SUCCESS;
	    }

	    private String getValue(String[] values, Map<String, Integer> columnIndexMap, String column) {
	        Integer index = columnIndexMap.get(column);
	        return (index != null && index < values.length) ? values[index].trim() : "";
	    }

	    @Reference
	    private ObjectDefinitionLocalService _objectDefinitionLocalService;

	    @Reference
	    private ObjectEntryLocalService _objectEntryLocalService;


	@Override
	public BackgroundTaskDisplay getBackgroundTaskDisplay(BackgroundTask backgroundTask) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BackgroundTaskExecutor clone() {
		// TODO Auto-generated method stub
		 return this;
	}

}
