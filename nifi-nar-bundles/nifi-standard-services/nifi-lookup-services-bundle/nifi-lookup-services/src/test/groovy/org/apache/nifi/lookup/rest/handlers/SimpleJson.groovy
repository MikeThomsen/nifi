package org.apache.nifi.lookup.rest.handlers

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import static groovy.json.JsonOutput.*

import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

class SimpleJson extends HttpServlet {
    Logger logger = LoggerFactory.getLogger(SimpleJson.class);
    @Override
    void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        String u = request.getHeader("X-USER")
        String p = request.getHeader("X-PASS")

        response.contentType = "application/json"
        response.outputStream.write(prettyPrint(
            toJson([
                username: u ?: "john.smith",
                password: p ?: "testing1234"
            ])
        ).bytes)
    }
}
