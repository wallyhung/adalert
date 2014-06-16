package com.alert.config;

import org.tanukisoftware.wrapper.WrapperListener;
import org.tanukisoftware.wrapper.WrapperManager;

public class Service implements WrapperListener{
	private static Application app = new Application();

	@Override
	public void controlEvent(int event) {
		if ((event == WrapperManager.WRAPPER_CTRL_LOGOFF_EVENT)
                && (WrapperManager.isLaunchedAsService() || WrapperManager.isIgnoreUserLogoffs())) {
            // Ignore
        } else {
            WrapperManager.stop(0);
            // Will not get here.
        }
	}

	@Override
	public Integer start(String[] arg0) {
		app.start();
		return null;
	}

	@Override
	public int stop(int event) {
		app.stop();
		return event;
	}
	
	public static void main(String[] args) throws InterruptedException {
		WrapperManager.start(new Service(), args);
	}
	

}
