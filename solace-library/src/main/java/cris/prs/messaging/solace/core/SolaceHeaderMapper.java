package cris.prs.messaging.solace.core;

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.XMLMessage;

import java.util.Map;

/** Maps Spring message headers onto Solace native fields and SDT user properties, and back. */
public interface SolaceHeaderMapper {

    void fromHeaders(Map<String, Object> headers, XMLMessage message);

    Map<String, Object> toHeaders(BytesXMLMessage message);
}
