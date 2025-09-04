package csv_listner.portlet;


import com.liferay.portal.kernel.log.Log;
import com.liferay.portal.kernel.log.LogFactoryUtil;
import com.liferay.portal.kernel.messaging.Message;
import com.liferay.portal.kernel.messaging.MessageListener;
import com.liferay.portal.kernel.messaging.MessageListenerException;
import org.osgi.service.component.annotations.Component;

	@Component(
	    immediate = true,
	    property = "destination.name=csv/file/processor",
	    service = MessageListener.class
	)
	public class CSVFileProcessorTest  implements MessageListener {

	    private static final Log _log = LogFactoryUtil.getLog(CSVFileProcessorTest.class);

	    @Override
	    public void receive(Message message) throws MessageListenerException {
	        _log.info(">>> CSVFileProcessorTest received message: " + message);
	        long fileEntryId = message.getLong("fileEntryId");
	        _log.info(">>> fileEntryId: " + fileEntryId);
	    }
}
