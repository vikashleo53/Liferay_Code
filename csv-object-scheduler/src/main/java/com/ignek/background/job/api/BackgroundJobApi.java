package com.ignek.background.job.api;

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

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

@Component(immediate = true, property = {
		"background.task.executor.class.name=com.ignek.background.job.api.BackgroundJobApi" }, service = BackgroundTaskExecutor.class)
public class BackgroundJobApi extends BaseBackgroundTaskExecutor {

	private static final Log _log = LogFactoryUtil.getLog(BackgroundJobApi.class);

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

		try (InputStream inputStream = fileVersion.getContentStream(false);
				BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
				CSVParser csvParser = CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true)
						.setIgnoreEmptyLines(true).setTrim(true).build().parse(reader)) {
			for (CSVRecord csvRecord : csvParser) {
				String studentName = csvRecord.get("studentName");
				String rollNumber = csvRecord.get("rollNumber");

				ObjectDefinition objectDefinition = _objectDefinitionLocalService
						.fetchObjectDefinitionByExternalReferenceCode("student", companyId);

				if (objectDefinition == null) {
					_log.error("Object definition 'student' not found.");
					continue;
				}

				Map<String, Serializable> fields = new HashMap<>();
				fields.put("studentName", studentName);
				fields.put("rollNumber", rollNumber);

				ServiceContext serviceContext = new ServiceContext();
				serviceContext.setScopeGroupId(groupId); // Use 0 if object is company scoped

				_objectEntryLocalService.addObjectEntry(userId, 0, objectDefinition.getObjectDefinitionId(), fields,
						serviceContext);

				_log.info("Created ObjectEntry for: " + studentName + " - " + rollNumber);
			}
		}

		return BackgroundTaskResult.SUCCESS;
	}

	@Override
	public BackgroundTaskDisplay getBackgroundTaskDisplay(BackgroundTask backgroundTask) {
		return null;
	}

	@Override
	public BackgroundTaskExecutor clone() {
		return this;
	}


	@Reference
	private ObjectDefinitionLocalService _objectDefinitionLocalService;

	@Reference
	private ObjectEntryLocalService _objectEntryLocalService;
}