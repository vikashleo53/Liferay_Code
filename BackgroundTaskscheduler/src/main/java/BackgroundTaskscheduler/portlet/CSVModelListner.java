package BackgroundTaskscheduler.portlet;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.osgi.service.component.annotations.Component;

import com.liferay.document.library.kernel.model.DLFileEntry;
import com.liferay.portal.background.task.service.BackgroundTaskLocalServiceUtil;
import com.liferay.portal.kernel.exception.ModelListenerException;
import com.liferay.portal.kernel.log.Log;
import com.liferay.portal.kernel.log.LogFactoryUtil;
import com.liferay.portal.kernel.model.BaseModelListener;
import com.liferay.portal.kernel.model.ModelListener;
import com.liferay.portal.kernel.service.ServiceContext;
@Component(
		immediate = true, 
		service = ModelListener.class
	)
public class CSVModelListner extends BaseModelListener<DLFileEntry>{
	 private static final Log _log = LogFactoryUtil.getLog(CSVModelListner.class);
	 
	 @Override
	 public void onAfterUpdate(DLFileEntry originalModel, DLFileEntry fileEntry) throws ModelListenerException {
	     String fileName = fileEntry.getFileName();

	     if (!fileName.endsWith(".csv")) {
	         return;
	     }

	     try {
	         long userId = fileEntry.getUserId();
	         long groupId = fileEntry.getGroupId();
	         long companyId = fileEntry.getCompanyId();
	         long fileEntryId = fileEntry.getFileEntryId();

	         Map<String, Serializable> taskContextMap = new HashMap<>();
	         taskContextMap.put("fileEntryId", fileEntryId);
	         taskContextMap.put("groupId", groupId);
	         taskContextMap.put("userId", userId);
	         taskContextMap.put("companyId", companyId);

				
				 BackgroundTaskLocalServiceUtil.addBackgroundTask( userId, groupId,
				  "CSV Import Task", CSVBackgroundTaskExecutor.class.getName(), taskContextMap,
				 new ServiceContext());
				 
	         _log.info("Background task created for CSV processing");

	     } catch (Exception e) {
	         _log.error("Error submitting background task", e);
	     }
	 }
	 

}
