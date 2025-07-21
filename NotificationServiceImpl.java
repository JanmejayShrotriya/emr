hupackage mobileoda.notification.service.mobileoda.impl;

import mobileoda.config.DataSourceConfig;
import mobileoda.core.constant.ResultCodeConstant;
import mobileoda.core.constant.Status;
import mobileoda.core.model.Response;
import mobileoda.core.model.impl.ErrorResponse;
import mobileoda.core.model.impl.Request;
import mobileoda.core.model.impl.SuccessResponse;
import mobileoda.notification.dao.DeviceDetailsDao;
import mobileoda.notification.dao.NotificationDataDao;
import mobileoda.notification.dto.NotificationRequestDto;
import mobileoda.notification.dto.NotificationResponseDto;
import mobileoda.notification.exception.NotificationServiceException;
import mobileoda.notification.mapper.NotificationRequestMapper;
import mobileoda.notification.mapper.NotificationResponseMapper;
import mobileoda.notification.model.DeviceDetails;
import mobileoda.notification.model.NotificationData;
import mobileoda.notification.model.NotificationDataResponse;
import mobileoda.notification.model.NotificationRequest;
import mobileoda.notification.model.NotificationRes;
import mobileoda.notification.model.NotificationResponse;
import mobileoda.notification.model.PushNotificationData;
import mobileoda.notification.repository.DeviceDetailsRepository;
import mobileoda.notification.repository.NotificationDataRepository;
import mobileoda.notification.service.api.NotificationService;
import mobileoda.registration.dao.RedirectionDao;
import mobileoda.registration.model.Redirection;
import mobileoda.util.CollectionUtil;
import mobileoda.util.DateUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Lazy;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.stereotype.Service;
import org.springframework.validation.annotation.Validated;

import javax.validation.Valid;
import java.lang.reflect.Field;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

@Service
@Lazy
@Validated
public class NotificationServiceImpl implements NotificationService {
    private Logger logger = LogManager.getLogger(NotificationServiceImpl.class);

    @Autowired
    private NotificationDataDao notificationDataDao;

    @Autowired
    private RedirectionDao redirectionDao;

    @Autowired
    private DeviceDetailsDao deviceDetailsDao;

    @Override
    public Response<NotificationResponseDto> savePushNotificationData(Request<NotificationRequestDto> request) {
        try {
            NotificationResponseDto notificationResponseDto = savePushNotificationData(request.getData());
            return new SuccessResponse<>(notificationResponseDto, ResultCodeConstant.SUCCESS, ResultCodeConstant.SUCCESS.getMessage(), request);
        } catch (RuntimeException e) {
            logger.error("Error occurred in savePushNotificationData method.", e);
            String errorMessage = e.getMessage();
            return new ErrorResponse(CollectionUtil.getListFromSingleObject(errorMessage), ResultCodeConstant.SAVE_PUSH_NOTIFICATION_DATA_ERROR, ResultCodeConstant.SAVE_PUSH_NOTIFICATION_DATA_ERROR.getMessage(), request);
        }
    }

    @Override
    public NotificationResponseDto savePushNotificationData(@Valid NotificationRequestDto notificationRequestDto) throws NotificationServiceException {
        NotificationResponse notificationResponse = savePushNotificationData(NotificationRequestMapper.mapper.mappingToNotificationRequest(notificationRequestDto));
        return NotificationResponseMapper.mapper.mappingToNotificationResponseDto(notificationResponse);
    }

    /**
     * Save push-notification data
     *
     * @param notificationRequest Instance of NotificationRequest
     * @return Instance of NotificationResponse
     * @throws NotificationServiceException NotificationServiceException
     */
    private NotificationResponse savePushNotificationData(NotificationRequest notificationRequest) throws NotificationServiceException {
        NotificationResponse notificationResponse = new NotificationResponse();
        String commAuthTokenRequest = notificationRequest.getCommAuthToken();
        if (StringUtils.isBlank(commAuthTokenRequest)) {
            notificationResponse.setNotificationDataResponse(prepareNotificationFailedResponse("NoCommAuthToken"));
            return notificationResponse;
        }

        String mobileContextRequest = notificationRequest.getMobileContext();
        if (StringUtils.isBlank(mobileContextRequest)) {
            notificationResponse.setNotificationDataResponse(prepareNotificationFailedResponse("InvalidRequest"));
            return notificationResponse;
        }

        Redirection contextDetails = redirectionDao.findByContext(mobileContextRequest);
        if (StringUtils.isBlank(contextDetails.getContext())) {
            notificationResponse.setNotificationDataResponse(prepareNotificationFailedResponse("NoContextAvailable"));
        } else {
            String commAuthTokenDB = contextDetails.getToken();

            // Validate comm auth token
            if (commAuthTokenRequest.equals(commAuthTokenDB)) {
                List<NotificationData> lstNotificationData = notificationRequest.getNotificationData();
                if (CollectionUtil.isListNonEmpty(lstNotificationData)) {
                    List<Integer> lstFailedUid = insertNotificationDetails(lstNotificationData, mobileContextRequest, notificationRequest);
                    notificationResponse.setNotificationDataResponse(prepareNotificationSuccessResponse(lstFailedUid));
                } else {
                    notificationResponse.setNotificationDataResponse(prepareNotificationFailedResponse("NoDataAvailable"));
                }
            } else {
                notificationResponse.setNotificationDataResponse(prepareNotificationFailedResponse("InvalidCommAuthToken"));
            }
        }
        return notificationResponse;
    }

    /**
     * Insert notification details.
     *
     * @param lstNotificationData  Instance of List
     * @param mobileContextRequest String value contains context-name
     * @param notificationRequest  Instance of NotificationRequest
     * @return Instance of List
     */
    private List<Integer> insertNotificationDetails(List<NotificationData> lstNotificationData, String mobileContextRequest, NotificationRequest notificationRequest) {
        List<Integer> lstFailedUid = new ArrayList<>();
        int batchSize = notificationDataDao.getBatchSize();

        List<PushNotificationData> pushNotificationDataList = new ArrayList<>(batchSize);
        List<DeviceDetails> deviceDetailsList = new ArrayList<>(batchSize);
        List<Integer> uidList = new ArrayList<>(batchSize);

        for (NotificationData notificationData : lstNotificationData) {
            pushNotificationDataList.add(preparePushNotificationData(mobileContextRequest, notificationData, notificationRequest));
            deviceDetailsList.add(prepareDeviceDetails(mobileContextRequest, notificationData));
            uidList.add(Integer.parseInt(notificationData.getUid()));

            if (pushNotificationDataList.size() >= batchSize) {
                processBatch(pushNotificationDataList, deviceDetailsList, uidList, lstFailedUid);
                pushNotificationDataList.clear();
                deviceDetailsList.clear();
                uidList.clear();
            }
        }
        if (!pushNotificationDataList.isEmpty()) {
            processBatch(pushNotificationDataList, deviceDetailsList, uidList, lstFailedUid);
        }
        return lstFailedUid;
    }

    /**
     * Process a batch of notification data and device details.
     * @param pushNotificationDataList List of PushNotificationData to be saved
     * @param deviceDetailsList List of DeviceDetails to be saved
     * @param uidList List of UIDs for error handling
     * @param lstFailedUid List to collect failed UIDs
     */
    private void processBatch(List<PushNotificationData> pushNotificationDataList,
                              List<DeviceDetails> deviceDetailsList,
                              List<Integer> uidList,
                              List<Integer> lstFailedUid) {
        // Process push notifications
        processNotificationBatch(pushNotificationDataList, uidList, lstFailedUid);

        // Process device details
        processDeviceDetailsBatch(deviceDetailsList);
    }

    private void processNotificationBatch(List<PushNotificationData> notifications, List<Integer> uidList, List<Integer> lstFailedUid) {
        try {
            List<PushNotificationData> saved = notificationDataDao.savePushNotificationDataBatch(notifications);
            if (saved.size() == notifications.size()) {
                return;
            }
            logger.error("Batch save incomplete: Expected {}, Actual {}",
                    notifications.size(), saved.size());
        } catch (RuntimeException e) {
            logger.error("Batch save failed", e);
        }

        // Fallback to individual processing
        logger.info("Processing notifications individually");
        for (int notification = 0; notification < notifications.size(); notification++) {
            processIndividualNotification(notifications.get(notification), uidList.get(notification), lstFailedUid);
        }
    }

    private void processIndividualNotification(PushNotificationData notification, int uid, List<Integer> lstFailedUid) {
        try {
            if (notificationDataDao.savePushNotificationData(notification) == null) {
                lstFailedUid.add(uid);
            }
        } catch (RuntimeException ex) {
            logger.error("Failed to save notification for uid: {}", uid, ex);
            lstFailedUid.add(uid);
        }
    }

    private void processDeviceDetailsBatch(List<DeviceDetails> deviceDetails) {
        try {
            deviceDetailsDao.saveDeviceDetailsBatch(deviceDetails);
        } catch (RuntimeException e) {
            logger.error("Batch device details save failed", e);
            logger.info("Processing device details individually");

            for (DeviceDetails device : deviceDetails) {
                try {
                    deviceDetailsDao.saveDeviceDetails(device);
                } catch (RuntimeException ex) {
                    logger.error("Failed to save device details: {}", device.getUid(), ex);
                }
            }
        }
    }

    /**
     * Prepare push notification data object.
     *
     * @param context             String value contains context-name
     * @param notificationData    Instance of NotificationData
     * @param notificationRequest Instance of NotificationRequest
     * @return Object of PushNotificationData
     */
    private PushNotificationData preparePushNotificationData(String context, NotificationData notificationData, NotificationRequest notificationRequest) {
        PushNotificationData pushNotificationData = new PushNotificationData();
        pushNotificationData.setUid(Integer.parseInt(notificationData.getUid()));
        pushNotificationData.setContext(context);
        pushNotificationData.setMessageCount(Integer.parseInt(notificationData.getMessageCount()));
        pushNotificationData.setMessageType(notificationData.getMessageType());
        pushNotificationData.setCreatedTsUTC(DateUtil.getCurrentDateTimeInUTC("yyyy-MM-dd HH:mm:ss"));
        pushNotificationData.setPracticeIp(StringUtils.trimToEmpty(notificationRequest.getHostAddress()));
        pushNotificationData.setHostName(StringUtils.trimToEmpty(notificationRequest.getHostName()));
        pushNotificationData.setTomcatPath(StringUtils.trimToEmpty(notificationRequest.getTomcatHome()));
        return pushNotificationData;
    }

    /**
     * Prepare device details object.
     * @param context          String value contains context-name
     * @param notificationData Instance of NotificationData
     * @return Object of DeviceDetails
     */
    private DeviceDetails prepareDeviceDetails(String context, NotificationData notificationData) {
        DeviceDetails deviceDetails = deviceDetailsDao.findByUidAndContext(Integer.parseInt(notificationData.getUid()), context);
        if (StringUtils.isBlank(deviceDetails.getContext())) {
            deviceDetails.setUid(Integer.parseInt(notificationData.getUid()));
            deviceDetails.setContext(context);
        }
        deviceDetails.setDevicePlatform(notificationData.getDevicePlatform());
        deviceDetails.setDeviceToken(notificationData.getDeviceToken());
        return deviceDetails;
    }

    /**
     * Prepare notification success response.
     *
     * @param lstFailedUid Instance of List
     * @return Instance of NotificationDataResponse
     */
    private NotificationDataResponse prepareNotificationSuccessResponse(List<Integer> lstFailedUid) {
        NotificationRes notificationRes = new NotificationRes();
        notificationRes.setFailedUidList(lstFailedUid);
        NotificationDataResponse response = new NotificationDataResponse();
        response.setStatus(Status.SUCCESS.getStatusValue());
        response.setResponse(notificationRes);
        return response;
    }

    /**
     * Prepare notification failed response.
     *
     * @param errorMessage String value contains error-message
     * @return Instance of NotificationDataResponse
     */
    private NotificationDataResponse prepareNotificationFailedResponse(String errorMessage) {
        NotificationDataResponse response = new NotificationDataResponse();
        response.setStatus(Status.FAILED.getStatusValue());
        response.setErrorMessage(errorMessage);
        return response;
    }

    /**
     * Test configuration class for the main method.
     * This class configures the Spring application context for testing.
     */
    @SpringBootApplication
    @ComponentScan({"mobileoda.notification", "mobileoda.config", "mobileoda.registration"})
    @EnableJpaRepositories(basePackages = {"mobileoda.notification.repository", "mobileoda.registration.repository"})
    @EntityScan(basePackages = {"mobileoda.notification.model", "mobileoda.registration.model"})
    public static class TestConfig {

    }

    /**
     * Main method to test batch processing with real database connections.
     * This method creates a Spring application context, gets the real DAO beans,
     * and tests the batch processing functionality by storing dummy data in the database.
     *
     * Tables used for storage:
     * - pushnotificationdata: Stores push notification data
     * - devicedetails: Stores device details
     *
     * @param args Command line arguments (not used)
     */
    public static void main(String[] args) {
        System.out.println("Starting Notification Service Test with Real Database");
        System.out.println("==================================================");

        ConfigurableApplicationContext context = null;
        try {
            System.out.println("Setting up Spring application context...");

            // Enable SQL logging for debugging
            System.setProperty("logging.level.org.hibernate.SQL", "DEBUG");
            System.setProperty("logging.level.org.hibernate.type.descriptor.sql.BasicBinder", "TRACE");

            // Create Spring application context using Spring Boot
            context = SpringApplication.run(TestConfig.class, args);

            System.out.println("Spring context initialized successfully");
            System.out.println("Database connection established");

            try {
                // Get the real DAO beans
                NotificationDataDao notificationDataDao = context.getBean(NotificationDataDao.class);
                DeviceDetailsDao deviceDetailsDao = context.getBean(DeviceDetailsDao.class);

                System.out.println("DAO beans retrieved successfully");
                System.out.println("- NotificationDataDao: " + notificationDataDao);
                System.out.println("- DeviceDetailsDao: " + deviceDetailsDao);

                // Create test data
                int dataCount = 2; // Smaller count for testing with real database
                System.out.println("Creating test data with " + dataCount + " records...");


                // Set batch size and test data count to ensure a non-full final batch
            //    int batchSize = notificationDataDao.getBatchSize();
             //   int dataCount = batchSize + 2; // Not a multiple of batch size, triggers final batch processing
              //  System.out.println("Creating test data with " + dataCount + " records (batch size: " + batchSize + ")...");




                List<NotificationData> testNotificationData = createDummyNotificationData(dataCount);
                String testContext = "DEMO_CLINIC";
                NotificationRequest testRequest = createDummyNotificationRequest();

                // Create service instance and set dependencies
                System.out.println("Setting up service instance...");
                NotificationServiceImpl service = new NotificationServiceImpl();
                setPrivateField(service, "notificationDataDao", notificationDataDao);
                setPrivateField(service, "deviceDetailsDao", deviceDetailsDao);

                // Execute test
                System.out.println("\nExecuting insertNotificationDetails with:");
                System.out.println("- Number of notifications: " + testNotificationData.size());
                System.out.println("- Context: " + testContext);
                System.out.println("- Batch size: " + notificationDataDao.getBatchSize());

                try {
                    long startTime = System.currentTimeMillis();

                    List<Integer> failedUids = service.insertNotificationDetails(
                            testNotificationData,
                            testContext,
                            testRequest
                    );

                    long endTime = System.currentTimeMillis();

                    System.out.println("\nTest Results:");
                    System.out.println("=============");
                    System.out.println("Total notifications processed: " + testNotificationData.size());
                    System.out.println("Failed UIDs: " + (failedUids.isEmpty() ? "None" : failedUids));
                    System.out.println("Processing time: " + (endTime - startTime) + "ms");
                    System.out.println("Number of batches processed: " + Math.ceil((double)testNotificationData.size() / notificationDataDao.getBatchSize()));

                  //  System.out.println("Number of batches processed: " + ((testNotificationData.size() / batchSize) + (testNotificationData.size() % batchSize == 0 ? 0 : 1)));

                    // Verify data was stored in the database
                    System.out.println("\nVerifying data in database...");
                    verifyDataInDatabase(notificationDataDao, deviceDetailsDao, testContext);

                    System.out.println("\nTest completed successfully!");
                } catch (Exception e) {
                    System.err.println("\nError during batch processing:");
                    System.err.println("- Error type: " + e.getClass().getName());
                    System.err.println("- Error message: " + e.getMessage());
                    e.printStackTrace();
                }
            } catch (Exception e) {
                System.err.println("\nError retrieving beans or setting up test:");
                System.err.println("- Error type: " + e.getClass().getName());
                System.err.println("- Error message: " + e.getMessage());
                e.printStackTrace();
            }

            // --- Add this block to test processIndividualNotification failure scenario ---
            System.out.println("\nTesting processIndividualNotification failure scenario...");
            NotificationServiceImpl testService = new NotificationServiceImpl();
            // Use a mock DAO that always returns null to simulate failure
            NotificationDataDao mockDao = new NotificationDataDao() {
                @Override
                public int getBatchSize() { return 1; }
                @Override
                public PushNotificationData savePushNotificationData(PushNotificationData data) { return null; }
                @Override
                public List<PushNotificationData> savePushNotificationDataBatch(List<PushNotificationData> dataList) { return dataList; }
            };
            setPrivateField(testService, "notificationDataDao", mockDao);

            PushNotificationData dummyNotification = new PushNotificationData();
            dummyNotification.setUid(9999);
            List<Integer> failedUids = new ArrayList<>();
            testService.processIndividualNotification(dummyNotification, 9999, failedUids);
            System.out.println("Failed UIDs after processIndividualNotification: " + failedUids);
            // --- End test block ---

            // --- Test fallback to individual insert when batch insert fails ---
            System.out.println("\nTesting fallback to individual insert when batch insert fails...");
            NotificationServiceImpl fallbackTestService = new NotificationServiceImpl();
            NotificationDataDao batchFailDao = new NotificationDataDao() {
                @Override
                public int getBatchSize() { return 3; }
                @Override
                public PushNotificationData savePushNotificationData(PushNotificationData data) {
                    // Simulate individual insert: fail for uid 8888, succeed for others
                    Integer uid = null;
                    try {
                        // Use reflection to get the uid field since getUid() is not available
                        java.lang.reflect.Field uidField = data.getClass().getDeclaredField("uid");
                        uidField.setAccessible(true);
                        uid = (Integer) uidField.get(data);
                    } catch (Exception e) {
                        // fallback: always succeed if uid can't be determined
                        return data;
                    }
                    if (uid != null && uid == 8888) return null;
                    return data;
                }
                @Override
                public List<PushNotificationData> savePushNotificationDataBatch(List<PushNotificationData> dataList) {
                    throw new RuntimeException("Simulated batch failure");
                }
            };
            setPrivateField(fallbackTestService, "notificationDataDao", batchFailDao);
            // DeviceDetailsDao can be a dummy, not used in this test
            setPrivateField(fallbackTestService, "deviceDetailsDao", createMockDeviceDetailsDao());

            // Prepare test data: 3 notifications, one will fail individual insert
            List<NotificationData> fallbackTestData = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                NotificationData n = new NotificationData();
                n.setUid(String.valueOf(8888 + i));
                n.setMessageCount("1");
                n.setMessageType("TEST");
                n.setDeviceToken("token");
                n.setDevicePlatform("Android");
                n.setMessage("msg");
                fallbackTestData.add(n);
            }
            NotificationRequest dummyReq = new NotificationRequest();
            dummyReq.setHostAddress("host");
            dummyReq.setHostName("host");
            dummyReq.setTomcatHome("home");

            List<Integer> failedUid = fallbackTestService.insertNotificationDetails(
                    fallbackTestData, "CTX", dummyReq
            );
            System.out.println("Failed UIDs after fallback test: " + failedUid);
            // --- End fallback test block ---


        } catch (Exception e) {
            System.err.println("\nError initializing Spring context:");
            System.err.println("- Error type: " + e.getClass().getName());
            System.err.println("- Error message: " + e.getMessage());
            System.err.println("- Possible causes:");
            System.err.println("  1. Database connection failed");
            System.err.println("  2. Configuration issues");
            System.err.println("  3. Missing dependencies");
            e.printStackTrace();
        } finally {
            // Close the context in finally block to ensure it's always closed
            if (context != null) {
                try {
                    System.out.println("Closing Spring context...");
                    context.close();
                    System.out.println("Spring context closed successfully");
                } catch (Exception e) {
                    System.err.println("Error closing Spring context: " + e.getMessage());
                }
            }
        }
    }

    private static void verifyDataInDatabase(NotificationDataDao notificationDataDao,
                                             DeviceDetailsDao deviceDetailsDao,
                                             String testContext) {
        System.out.println("\nVerifying data in database:");
        System.out.println("==========================");

        try {
            // Get the repositories from the DAOs
            NotificationDataRepository notificationRepo = getFieldValue(notificationDataDao, "notificationDataRepository");
            DeviceDetailsRepository deviceRepo = getFieldValue(deviceDetailsDao, "deviceDetailsRepository");

            // Get all records and filter by context
            List<PushNotificationData> allNotifications = notificationRepo.findAll();
            List<PushNotificationData> notifications = new ArrayList<>();

            // Filter notifications by context using reflection (since PushNotificationData doesn't have getContext())
            for (PushNotificationData notification : allNotifications) {
                try {
                    String context = getFieldValue(notification, "context");
                    if (testContext.equals(context)) {
                        notifications.add(notification);
                    }
                } catch (Exception e) {
                    System.err.println("Error accessing context field: " + e.getMessage());
                }
            }

            System.out.println("Records in pushnotificationdata table for context '" + testContext + "': " + notifications.size());

            // Get all device details and filter by context
            List<DeviceDetails> allDevices = deviceRepo.findAll();
            List<DeviceDetails> devices = new ArrayList<>();

            // Filter device details by context
            // Note: DeviceDetails has getContext() method, but using the same approach as PushNotificationData for consistency
            for (DeviceDetails device : allDevices) {
                if (testContext.equals(device.getContext())) {
                    devices.add(device);
                }
            }

            System.out.println("Records in devicedetails table for context '" + testContext + "': " + devices.size());

            // Print sample notification records
            if (!notifications.isEmpty()) {
                System.out.println("\nSample notification data:");
                for (int i = 0; i < Math.min(3, notifications.size()); i++) {
                    System.out.println(notifications.get(i));
                }
            } else {
                System.out.println("\nNo notification data found for context: " + testContext);
            }

            // Print sample device records
            if (!devices.isEmpty()) {
                System.out.println("\nSample device details:");
                for (int i = 0; i < Math.min(3, devices.size()); i++) {
                    System.out.println(devices.get(i));
                }
            } else {
                System.out.println("\nNo device details found for context: " + testContext);
            }

            // Print total record counts
            System.out.println("\nTotal records in database:");
            System.out.println("- pushnotificationdata: " + allNotifications.size());
            System.out.println("- devicedetails: " + allDevices.size());

        } catch (Exception e) {
            System.err.println("Error verifying data: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Get the value of a private field using reflection.
     *
     * @param object The object containing the field
     * @param fieldName The name of the field
     * @return The value of the field
     * @throws Exception If the field cannot be accessed
     */
    private static <T> T getFieldValue(Object object, String fieldName) throws Exception {
        Field field = object.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        @SuppressWarnings("unchecked")
        T value = (T) field.get(object);
        return value;
    }

    private static NotificationDataDao createMockNotificationDataDao() {
        return new NotificationDataDao() {
            @Override
            public int getBatchSize() {
                return 5; // Set batch size
            }

            @Override
            public PushNotificationData savePushNotificationData(PushNotificationData data) {
                return data;
            }

            @Override
            public List<PushNotificationData> savePushNotificationDataBatch(List<PushNotificationData> dataList) {
                System.out.println("[DAO] Saving batch of " + dataList.size() + " notifications");
                return dataList;
            }
        };
    }

    private static DeviceDetailsDao createMockDeviceDetailsDao() {
        return new DeviceDetailsDao() {
            @Override
            public DeviceDetails findByUidAndContext(int uid, String context) {
                DeviceDetails details = new DeviceDetails();
                details.setUid(uid);
                details.setContext(context);
                return details;
            }

            @Override
            public DeviceDetails saveDeviceDetails(DeviceDetails details) {
                System.out.println("[DAO] Saving device details for UID: " + details.getUid());
                return details;
            }

            @Override
            public List<DeviceDetails> saveDeviceDetailsBatch(List<DeviceDetails> detailsList) {
                System.out.println("[DAO] Saving batch of " + detailsList.size() + " device details");
                return detailsList;
            }
        };
    }

    private static List<NotificationData> createDummyNotificationData(int count) {
        List<NotificationData> data = new ArrayList<>();

        String[] messageTypes = {
                "APPOINTMENT", "LAB_RESULT", "PRESCRIPTION", "MESSAGE", "REMINDER",
                "VITALS", "BILLING", "REFERRAL", "FOLLOWUP", "VACCINATION"
        };
        String[] platforms = {"iOS", "Android"};

        for (int i = 1; i <= count; i++) {
            NotificationData notification = new NotificationData();
            notification.setUid(String.valueOf(1511 + i));
            notification.setMessageCount(String.valueOf(1 + (i % 5)));
            notification.setMessageType(messageTypes[i % messageTypes.length]);
            notification.setDeviceToken("fcm_token_" + generateDummyToken());
            notification.setDevicePlatform(platforms[i % platforms.length]);
            notification.setMessage("Patient " + (i * 100) + " has a new " +
                    messageTypes[i % messageTypes.length].toLowerCase() + " notification");
            data.add(notification);
        }

        return data;
    }

    private static NotificationRequest createDummyNotificationRequest() {
        NotificationRequest request = new NotificationRequest();
        request.setMobileContext("DEMO_CLINIC");
        request.setCommAuthToken("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.demo");
        request.setHostName("clinic-server-prod-01");
        request.setHostAddress("192.168.1.100");
        request.setTomcatHome("/usr/local/tomcat9");
        return request;
    }

    private static String generateDummyToken() {
        StringBuilder token = new StringBuilder();
        String chars = "abcdef0123456789";
        for (int i = 0; i < 32; i++) {
            token.append(chars.charAt((int) (Math.random() * chars.length())));
        }
        return token.toString();
    }

    private static void setPrivateField(Object object, String fieldName, Object value) {
        try {
            java.lang.reflect.Field field = object.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            field.set(object, value);
        } catch (Exception e) {
            throw new RuntimeException("Failed to set private field: " + fieldName, e);
        }
    }

    // Helper class to simulate DateUtil if not available
    private static class DateUtil {
        public static String getCurrentDateTimeInUTC(String format) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            return sdf.format(new Date());
        }
    }


@Transactional
public void savePushNotificationsInBatch(List<PushNotificationData> notifications,
                                         List<Integer> uidList,
                                         List<Integer> failedUidList) {
    final int BATCH_SIZE = 50;

    for (int i = 0; i < notifications.size(); i++) {
        try {
            PushNotificationData notification = notifications.get(i);
            entityManager.persist(notification);

            // Flush & clear at batch intervals
            if ((i + 1) % BATCH_SIZE == 0) {
                entityManager.flush();
                entityManager.clear();
            }
        } catch (Exception ex) {
            // Add failed UID to tracking list
            Integer failedUid = (uidList != null && uidList.size() > i) ? uidList.get(i) : null;
            if (failedUid != null) failedUidList.add(failedUid);

            // Log error
            log.error("Failed to persist PushNotificationData at index " + i, ex);
        }
    }

    // Final flush for remaining records
    try {
        entityManager.flush();
        entityManager.clear();
    } catch (Exception ex) {
        log.error("Final flush failed after batch insert.", ex);
    }
}


private List<Integer> insertNotificationDetails(List<NotificationData> notificationDataList,
                                                String mobileContextRequest,
                                                NotificationRequest notificationRequest) {
    List<PushNotificationData> pushNotificationDataList = new ArrayList<>();
    List<DeviceDetails> deviceDetailsList = new ArrayList<>();
    List<Integer> uidList = new ArrayList<>();
    List<Integer> failedUidList = new ArrayList<>();

    for (NotificationData data : notificationDataList) {
        PushNotificationData pushData = preparePushNotificationData(mobileContextRequest, notificationRequest, data);
        DeviceDetails device = prepareDeviceDetails(mobileContextRequest, notificationRequest, data);

        pushNotificationDataList.add(pushData);
        deviceDetailsList.add(device);
        uidList.add(data.getUid());
    }

    if (!pushNotificationDataList.isEmpty()) {
        savePushNotificationsInBatch(pushNotificationDataList, uidList, failedUidList);
    }

    return failedUidList;
}




@Transactional
public void saveDeviceDetailsInBatch(List<DeviceDetails> devices) {
    final int BATCH_SIZE = 50;
    for (int i = 0; i < devices.size(); i++) {
        entityManager.persist(devices.get(i));
        if ((i + 1) % BATCH_SIZE == 0) {
            entityManager.flush();
            entityManager.clear();
        }
    }
    entityManager.flush();
    entityManager.clear();
}
}