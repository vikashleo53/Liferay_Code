package csv_listner.portlet;

import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import com.liferay.portal.kernel.log.Log;
import com.liferay.portal.kernel.log.LogFactoryUtil;
import com.liferay.portal.kernel.messaging.Destination;
import com.liferay.portal.kernel.messaging.DestinationConfiguration;
import com.liferay.portal.kernel.messaging.DestinationFactory;


@Component
	(immediate = true)

public class CSVFileDestinationRegistrar {
	private static final Log _log = LogFactoryUtil.getLog(csvFileListner.class);
	  private Destination _destination;

	    @Reference
	    private DestinationFactory _destinationFactory;

	    @Activate
	    protected void activate() {
	        DestinationConfiguration destinationConfiguration =
	            DestinationConfiguration.createSerialDestinationConfiguration(
	                "csv/file/processor");

	        _destination = _destinationFactory.createDestination(destinationConfiguration);
	        _log.info("detination created");
	    }

	    @Deactivate
	    protected void deactivate() {
	        if (_destination != null) {
	            _destination.destroy();
	        }
	    }
	    
}