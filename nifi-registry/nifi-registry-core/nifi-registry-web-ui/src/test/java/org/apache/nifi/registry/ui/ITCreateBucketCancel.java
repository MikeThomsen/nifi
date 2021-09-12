/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.registry.ui;

import io.github.bonigarcia.wdm.WebDriverManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;

import static org.junit.jupiter.api.Assertions.fail;

public class ITCreateBucketCancel {
    private WebDriver driver;
    private String baseUrl;
    private boolean acceptNextAlert = true;
    private WebDriverWait wait;
    private StringBuffer verificationErrors = new StringBuffer();

    @BeforeEach
    public void setUp() throws Exception {
        WebDriverManager.chromedriver().setup();
        driver = new ChromeDriver();
        baseUrl = "http://localhost:18080/nifi-registry";
        wait = new WebDriverWait(driver, 30);
    }

    @Test
    public void testCreateBucketCancel() {
        // go directly to settings by URL
        driver.get(baseUrl + "/#/administration/workflow");

        // wait for administration route to load
        wait.until(ExpectedConditions.visibilityOfElementLocated(By.cssSelector("[data-automation-id='no-buckets-message']")));

        // confirm new bucket button exists
        wait.until(ExpectedConditions.visibilityOfElementLocated(By.cssSelector("[data-automation-id='new-bucket-button']")));

        // select new bucket button
        WebElement newBucketButton = driver.findElement(By.cssSelector("[data-automation-id='new-bucket-button']"));
        newBucketButton.click();

        // wait for new bucket dialog
        wait.until(ExpectedConditions.visibilityOfElementLocated(By.cssSelector("#nifi-registry-admin-create-bucket-dialog")));

        // confirm bucket name field exists
        wait.until(ExpectedConditions.visibilityOfElementLocated(By.cssSelector("#nifi-registry-admin-create-bucket-dialog input")));

        // place cursor in bucket name field
        WebElement bucketNameInput = driver.findElement(By.cssSelector("#nifi-registry-admin-create-bucket-dialog input"));
        bucketNameInput.clear();

        // name the bucket ABC
        bucketNameInput.sendKeys("ABC");

        // confirm cancel button exists
        wait.until(ExpectedConditions.visibilityOfElementLocated(By.cssSelector("#nifi-registry-admin-create-bucket-dialog button.mat-fds-regular")));

        // select cancel button
        WebElement cancelButton = driver.findElement(By.cssSelector("#nifi-registry-admin-create-bucket-dialog button.mat-fds-regular"));
        cancelButton.click();

        // wait for new bucket dialog to close
        wait.until(ExpectedConditions.invisibilityOfElementLocated(By.cssSelector("#nifi-registry-admin-create-bucket-dialog")));

        // confirm no buckets exist
        wait.until(ExpectedConditions.visibilityOfElementLocated(By.cssSelector("[data-automation-id='no-buckets-message']")));
    }

    @AfterEach
    public void tearDown() {
        driver.quit();
        String verificationErrorString = verificationErrors.toString();
        if (!"".equals(verificationErrorString)) {
            fail(verificationErrorString);
        }
    }
}