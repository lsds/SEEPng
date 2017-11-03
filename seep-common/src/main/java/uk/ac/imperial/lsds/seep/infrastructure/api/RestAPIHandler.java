package uk.ac.imperial.lsds.seep.infrastructure.api;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.util.MultiMap;

import com.fasterxml.jackson.databind.ObjectMapper;

public class RestAPIHandler extends AbstractHandler {

    public static final ObjectMapper mapper = new ObjectMapper();

    private Map<String, RestAPIRegistryEntry> restAPIRegistry;

    public RestAPIHandler(Map<String, RestAPIRegistryEntry> restAPIRegistry) {
        this.restAPIRegistry = restAPIRegistry;
    }

    public static Map<String, String> getReqParameter(String query) {
        String[] params = query.split("&");
        Map<String, String> map = new HashMap<>();
        for (String param : params) {
            String name = param.split("=")[0];
            String value = param.split("=")[1];
            map.put(name, value);
        }
        return map;
    }

    @Override
    public void handle(String target, Request baseRequest,
                       HttpServletRequest request, HttpServletResponse response)
            throws IOException, ServletException {

        String callback = request.getParameter("callback");

        response.setContentType("application/json;charset=utf-8");
        response.setStatus(HttpServletResponse.SC_OK);
        response.setHeader("Access-Control-Allow-Origin", "*");
        response.setHeader("Access-Control-Allow-Methods", "GET, POST, DELETE, PUT");

        if (!this.restAPIRegistry.containsKey(target) && !RestAPIManager.restAPIRegistry.containsKey(target)) {
            baseRequest.setHandled(true);
            if (callback != null)
                response.getWriter().println(callback + "(" + mapper.writeValueAsString(this.restAPIRegistry.keySet()) + "," + mapper.writeValueAsString(RestAPIManager.restAPIRegistry.keySet()) + ")");
            else
                response.getWriter().println(mapper.writeValueAsString(this.restAPIRegistry.keySet()) + "," + mapper.writeValueAsString(RestAPIManager.restAPIRegistry.keySet()));
        }
        else {
        	Map<String, RestAPIRegistryEntry> theDecider = this.restAPIRegistry;
        	if (!this.restAPIRegistry.containsKey(target)) {
        		theDecider = RestAPIManager.restAPIRegistry;
        	}
            if (baseRequest.getMethod().equals("GET")) {
                baseRequest.setHandled(true);

                // Obtain the MultiMap req parameter
                MultiMap<String> reqParametersMultiMap = new MultiMap<>();
                Map<String, String[]> reqParametersArray = baseRequest.getParameterMap();

                for (Map.Entry<String, String[]> entry : reqParametersArray.entrySet()) {
                    String key = entry.getKey();
                    String[] value = entry.getValue();

                    reqParametersMultiMap.addValues(key, value);
                }

                if (callback != null)
                    response.getWriter().println(callback + "(" + mapper.writeValueAsString(theDecider.get(target).getAnswer(reqParametersMultiMap)) + ")");
                else
                    response.getWriter().println(mapper.writeValueAsString(theDecider.get(target).getAnswer(reqParametersMultiMap)));
            }
            else if (baseRequest.getMethod().equals("POST")) {
                baseRequest.setHandled(true);

                // Obtain the MultiMap req parameter
                MultiMap<String> reqParametersMultiMap = new MultiMap<>();
                Map<String, String[]> reqParametersArray = baseRequest.getParameterMap();

                for (Map.Entry<String, String[]> entry : reqParametersArray.entrySet()) {
                    String key = entry.getKey();
                    String[] value = entry.getValue();

                    reqParametersMultiMap.addValues(key, value);
                }

                if (callback != null)
                    response.getWriter().println(callback + "(" + mapper.writeValueAsString(theDecider.get(target).getAnswer(reqParametersMultiMap)) + ")");
                else
                    response.getWriter().println(mapper.writeValueAsString(theDecider.get(target).getAnswer(reqParametersMultiMap)));
            }
        }
    }
}
