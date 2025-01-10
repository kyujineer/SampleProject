package com.thingworx.weather;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URLEncoder;
import java.util.Iterator;

import org.json.simple.*;
import org.json.simple.parser.JSONParser;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.joda.time.DateTime;
import org.slf4j.Logger;

import com.thingworx.data.util.InfoTableInstanceFactory;
import com.thingworx.logging.LogUtilities;
import com.thingworx.metadata.annotations.ThingworxBaseTemplateDefinition;
import com.thingworx.metadata.annotations.ThingworxConfigurationTableDefinition;
import com.thingworx.metadata.annotations.ThingworxConfigurationTableDefinitions;
import com.thingworx.metadata.annotations.ThingworxDataShapeDefinition;
import com.thingworx.metadata.annotations.ThingworxEventDefinition;
import com.thingworx.metadata.annotations.ThingworxEventDefinitions;
import com.thingworx.metadata.annotations.ThingworxFieldDefinition;
import com.thingworx.metadata.annotations.ThingworxPropertyDefinition;
import com.thingworx.metadata.annotations.ThingworxPropertyDefinitions;
import com.thingworx.metadata.annotations.ThingworxServiceDefinition;
import com.thingworx.metadata.annotations.ThingworxServiceParameter;
import com.thingworx.metadata.annotations.ThingworxServiceResult;
import com.thingworx.metadata.annotations.ThingworxSubscription;
import com.thingworx.metadata.annotations.ThingworxSubscriptions;
import com.thingworx.system.ContextType;
import com.thingworx.things.Thing;
import com.thingworx.things.events.ThingworxEvent;
import com.thingworx.types.InfoTable;
import com.thingworx.types.collections.ValueCollection;
import com.thingworx.types.primitives.NumberPrimitive;
import com.thingworx.types.primitives.StringPrimitive;

@ThingworxBaseTemplateDefinition(name = "GenericThing")
@ThingworxPropertyDefinitions(properties = {
		@ThingworxPropertyDefinition(name = "CurrentCity", description = "", category = "", baseType = "STRING", isLocalOnly = false, aspects = {
				"defaultValue:Seoul", "isPersistent:true", "dataChangeType:VALUE" }), @ThingworxPropertyDefinition(name = "Temperature", description = "", category = "", baseType = "NUMBER", isLocalOnly = false, aspects = {
				"dataChangeType:VALUE" }), @ThingworxPropertyDefinition(name = "WeatherDescription", description = "", category = "", baseType = "STRING", isLocalOnly = false, aspects = {
						"dataChangeType:VALUE" }) })
@ThingworxConfigurationTableDefinitions(tables = {
		@ThingworxConfigurationTableDefinition(name = "OpenWeatherMapConfigurationTable", description = "", isMultiRow = false, ordinal = 0, dataShape = @ThingworxDataShapeDefinition(fields = {
				@ThingworxFieldDefinition(name = "appid", description = "", baseType = "STRING", ordinal = 0, aspects = {
						"isRequired:true" }) })) })
@ThingworxEventDefinitions(events = {
		@ThingworxEventDefinition(name = "BadWeather", description = "", category = "", dataShape = "Weather", isInvocable = true, isPropertyEvent = false, isLocalOnly = false, aspects = {}) })
@ThingworxSubscriptions(subscriptions = {
		@ThingworxSubscription(source = "", eventName = "BadWeather", sourceProperty = "", handler = "HandleWeather", enabled = true) })
public class WeatherThingTemplate extends Thing {

	public WeatherThingTemplate() {
		// TODO Auto-generated constructor stub
		
	}

	private static Logger _logger = LogUtilities.getInstance().getApplicationLogger(WeatherThingTemplate.class);
	private String _appid;
	@Override
	public void initializeThing(ContextType contextType) throws Exception {
	     super.initializeThing(contextType);
	     _appid = (String) this.getConfigurationSetting("OpenWeatherMapConfigurationTable", "appid");
	}
	@ThingworxServiceDefinition(name = "UpdateWeatherInfo", description = "", category = "", isAllowOverride = false, aspects = {
			"isAsync:false" })
	@ThingworxServiceResult(name = "Result", description = "", baseType = "NOTHING", aspects = {})
	public void UpdateWeatherInfo(
			@ThingworxServiceParameter(name = "City", description = "", baseType = "STRING") String City) throws Exception {
		
		_logger.trace("Entering Service: UpdateWeatherInfo");
        String cityProp = this.getPropertyValue("CurrentCity").getStringValue();
        if (City == null){
            City = cityProp;
        } else {
            this.setPropertyValue("CurrentCity", new StringPrimitive(City));
        }
        
        String url = "http://api.openweathermap.org/data/2.5/weather?q=" +URLEncoder.encode(City,"UTF-8") + "&appid="+ _appid+"&units=imperial";
        
        // create a http client
        HttpClient client = new DefaultHttpClient();
        
        // create a get request with the URL
        HttpGet getRequest = new HttpGet(url);
        
        // add Accept header to accept json format response
        getRequest.addHeader("Accept", "application/json");
        
        // send the get request and obtain a response
        HttpResponse response = client.execute(getRequest);
        
        // if response is successful the status code will be 200.
        if (response.getStatusLine().getStatusCode() == 200) {
            BufferedReader br = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
            StringBuilder sb = new StringBuilder();
            String line = "";
            while ((line = br.readLine()) != null) {
                sb.append(line);
            }
            JSONParser parser = new JSONParser();
            JSONObject json = (JSONObject) parser.parse(sb.toString());
            JSONArray weather = (JSONArray) json.get("weather");
            Iterator<JSONObject> it = weather.iterator();
            String description = (String) it.next().get("description");
            this.setPropertyValue("WeatherDescription", new StringPrimitive(description));
            double temp = (Double) ((JSONObject) json.get("main")).get("temp");
            this.setPropertyValue("Temperature", new NumberPrimitive(temp));

            /* fire event BadWeather */
            if (description.contains("snow") || description.contains("rain") || description.contains("thunderstorm")) {
                ValueCollection v = new ValueCollection();
                v.put("weatherDescription", new StringPrimitive(description));
                InfoTable data = InfoTableInstanceFactory.createInfoTableFromDataShape("Weather");
                data.addRow(v);
                _logger.info("Firing event");
                ThingworxEvent event = new ThingworxEvent();
                event.setEventName("BadWeather");
                event.setEventData(data);
                this.dispatchEvent(event);
            }
        }
        
        
		
	}
	@ThingworxServiceDefinition(name = "HandleWeather", description = "Subscription handler", category = "", isAllowOverride = false, aspects = {
			"isAsync:false" })
	public void HandleWeather(
			@ThingworxServiceParameter(name = "eventData", description = "", baseType = "INFOTABLE") InfoTable eventData,
			@ThingworxServiceParameter(name = "eventName", description = "", baseType = "STRING") String eventName,
			@ThingworxServiceParameter(name = "eventTime", description = "", baseType = "DATETIME") DateTime eventTime,
			@ThingworxServiceParameter(name = "source", description = "", baseType = "STRING") String source,
			@ThingworxServiceParameter(name = "sourceProperty", description = "", baseType = "STRING") String sourceProperty) throws Exception{
		_logger.trace("Entering Service: HandleWeather with: Source: \"\", Event: \"BadWeather\", Property: \"\"");
	     this.setPropertyValue("AlertNotification", new StringPrimitive("Alert:"+eventData.getFirstRow().getStringValue("weatherDescription")));
	     _logger.trace("Exiting Service: HandleWeather");
	}
	
}
