package com.example.servicelab;

import android.app.Notification;
import android.app.NotificationChannel;
import android.app.NotificationManager;
import android.app.PendingIntent;
import android.app.Service;
import android.content.Intent;
import android.os.Build;
import android.os.Handler;
import android.os.IBinder;
import android.os.Looper;
import android.util.Log;

import androidx.annotation.Nullable;
import androidx.core.app.NotificationCompat;

import org.greenrobot.eventbus.Subscribe;
import org.greenrobot.eventbus.ThreadMode;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import br.ufma.lsdi.cdal.models.locals.SensorData;
import br.ufma.lsdi.cdal.services.AdaptationService;
import br.ufma.lsdi.cdal.services.S2PAService;
import br.ufma.lsdi.cdal.util.MHubEventBus;
import br.ufma.lsdi.cddl.CDDL;
import br.ufma.lsdi.cddl.ConnectionFactory;
import br.ufma.lsdi.cddl.listeners.IConnectionListener;
import br.ufma.lsdi.cddl.message.Message;
import br.ufma.lsdi.cddl.message.SensorDataMessage;
import br.ufma.lsdi.cddl.network.ConnectionImpl;
import br.ufma.lsdi.cddl.pubsub.Publisher;
import br.ufma.lsdi.cddl.pubsub.PublisherFactory;
import br.ufma.lsdi.cddl.pubsub.Subscriber;
import br.ufma.lsdi.cddl.pubsub.SubscriberFactory;
import br.ufma.lsdi.cddl.services.CommandServiceImpl;

public class CDDLService extends Service {

    // ========================================================================
    // CONSTANTES DE CONFIGURAÇÃO E IDENTIFICAÇÃO
    // ========================================================================

    private static final String TAG = "CDDLServiceLab";

    public static final String CHANNEL_ID = "ServiceLAB_Channel";
    public static final String GROUP_KEY_FOREGROUND_SERVICE = "cddl_service_group";

    private static final String TARGET_SENSOR = "HMSoft";
    private static final String EXTERNAL_BROKER = "lsdi.ufma.br";

    private static final boolean BLE_RESET_ON_DISCONNECT_ENABLED = true;
    private static final long REACQUIRE_DELAY_MS = 5000;
    private static final long BLE_RESTART_DELAY_MS = 3000;
    private static final long BROKER_UNAVAILABLE_LOG_INTERVAL_MS = 10000;

    // ========================================================================
    // THREADS E HANDLERS
    // ========================================================================

    private final ExecutorService cddlExecutor = Executors.newSingleThreadExecutor();
    private final Handler mainHandler = new Handler(Looper.getMainLooper());
    private final Handler watchdogHandler = new Handler(Looper.getMainLooper());
    private final Handler bleResetHandler = new Handler(Looper.getMainLooper());

    // ========================================================================
    // ESTADO DO SERVIÇO
    // ========================================================================

    private final Set<String> activeHmSoftMouuids =
            Collections.synchronizedSet(new HashSet<>());

    private final String localPublisherId = "MHUB_SALA_ETS_" + System.currentTimeMillis();

    private CDDL cddl;

    private ConnectionImpl conLocal;
    private ConnectionImpl conExterno;

    private Publisher publisherLocal;
    private Publisher publisherExterno;

    private Subscriber subscriberCommandExternal;

    /*
     * Mantemos a lógica do CommandService, mas sem deixar o Android iniciar
     * br.ufma.lsdi.cddl.services.CommandService como Service independente.
     * Isso evita reinício automático com CDDL.getConnection() nulo após ANR.
     */
    private CommandServiceImpl commandServiceImpl;

    private volatile boolean foregroundStarted = false;
    private volatile boolean localConnectionReady = false;
    private volatile boolean cdalStarted = false;
    private volatile boolean commandServiceStarted = false;
    private volatile boolean bleRestartInProgress = false;
    private volatile long lastBrokerUnavailableLogMs = 0L;

    // ========================================================================
    // CICLO DE VIDA DO SERVICE
    // ========================================================================

    @Override
    public void onCreate() {
        super.onCreate();

        createNotificationChannel();
        startForegroundServiceNotification();

        initLocalMicroBrokerAndCDDL();
        initConnectExternalBroker();
    }

    @Override
    public int onStartCommand(Intent intent, int flags, int startId) {
        return START_STICKY;
    }

    @Override
    public void onDestroy() {
        Log.d(TAG, "Encerrando serviço e limpando conexões...");

        watchdogHandler.removeCallbacksAndMessages(null);
        bleResetHandler.removeCallbacksAndMessages(null);
        mainHandler.removeCallbacksAndMessages(null);

        try {
            if (MHubEventBus.getDefault().isRegistered(this)) {
                MHubEventBus.getDefault().unregister(this);
            }
        } catch (Exception e) {
            Log.e(TAG, "Erro ao remover listener do MHubEventBus: " + e.getMessage(), e);
        }

        try {
            if (commandServiceImpl != null) {
                commandServiceImpl.close();
            }
        } catch (Exception e) {
            Log.e(TAG, "Erro ao encerrar CommandServiceImpl: " + e.getMessage(), e);
        }

        try {
            if (cddl != null) {
                cddl.stopSensor(TARGET_SENSOR);
                cddl.stopCommunicationTechnology(CDDL.BLE_TECHNOLOGY_ID);
            }
        } catch (Exception e) {
            Log.e(TAG, "Erro ao parar BLE/HMSoft: " + e.getMessage(), e);
        }

        try {
            stopService(new Intent(this, S2PAService.class));
            stopService(new Intent(this, AdaptationService.class));
        } catch (Exception e) {
            Log.e(TAG, "Erro ao parar serviços CDAL: " + e.getMessage(), e);
        }

        try {
            if (conLocal != null) {
                conLocal.unsubscribeAll();
                conLocal.disconnect();
            }
        } catch (Exception e) {
            Log.e(TAG, "Erro ao desconectar microbroker local: " + e.getMessage(), e);
        }

        cleanupExternalBrokerConnection();

        try {
            CDDL.stopMicroBroker();
            Log.d(TAG, "MicroBroker local encerrado com sucesso.");
        } catch (Exception e) {
            Log.e(TAG, "Erro ao parar MicroBroker local: " + e.getMessage(), e);
        }

        activeHmSoftMouuids.clear();

        commandServiceImpl = null;
        subscriberCommandExternal = null;
        publisherLocal = null;
        publisherExterno = null;
        conLocal = null;
        conExterno = null;
        cddl = null;

        try {
            cddlExecutor.shutdownNow();
        } catch (Exception e) {
            Log.e(TAG, "Erro ao encerrar executor CDDL: " + e.getMessage());
        }

        super.onDestroy();
    }

    @Nullable
    @Override
    public IBinder onBind(Intent intent) {
        return null;
    }

    // ========================================================================
    // NOTIFICAÇÃO DO FOREGROUND SERVICE
    // ========================================================================

    private void startForegroundServiceNotification() {
        Notification notification = buildNotification(
                "ServiceLab em execução",
                "Coletando dados BLE e publicando no broker externo."
        );

        startForeground(1, notification);
        foregroundStarted = true;
    }

    private void showBrokerDisconnectedNotification(String text) {
        Notification notification = buildNotification(
                "Broker externo desconectado",
                text
        );

        NotificationManager manager = getSystemService(NotificationManager.class);

        if (!foregroundStarted) {
            startForeground(1, notification);
            foregroundStarted = true;
        } else if (manager != null) {
            manager.notify(1, notification);
        }
    }

    private Notification buildNotification(String title, String text) {
        Intent notificationIntent = new Intent(this, MainActivity.class);

        PendingIntent pendingIntent = PendingIntent.getActivity(
                this,
                0,
                notificationIntent,
                PendingIntent.FLAG_IMMUTABLE
        );

        return new NotificationCompat.Builder(this, CHANNEL_ID)
                .setContentTitle(title)
                .setContentText(text)
                .setSmallIcon(R.drawable.logo_lsdi_round)
                .setContentIntent(pendingIntent)
                .setCategory(NotificationCompat.CATEGORY_SERVICE)
                .setGroup(GROUP_KEY_FOREGROUND_SERVICE)
                .setPriority(NotificationCompat.PRIORITY_LOW)
                .setOngoing(true)
                .setAutoCancel(false)
                .build();
    }

    private void createNotificationChannel() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            NotificationChannel serviceChannel = new NotificationChannel(
                    CHANNEL_ID,
                    "CDDL Service Lab",
                    NotificationManager.IMPORTANCE_LOW
            );

            serviceChannel.setDescription("Canal para notificação do Serviço CDDL de Telemetria");

            NotificationManager manager = getSystemService(NotificationManager.class);

            if (manager != null) {
                manager.createNotificationChannel(serviceChannel);
            }
        }
    }

    // ========================================================================
    // INICIALIZAÇÃO LOCAL: MICROBROKER + CDDL + CDAL + COMMANDSERVICEIMPL
    // ========================================================================

    private void initLocalMicroBrokerAndCDDL() {
        cddlExecutor.execute(() -> {
            try {
                String host = CDDL.startMicroBroker();

                conLocal = ConnectionFactory.createConnection();
                conLocal.setClientId(localPublisherId + "_LOCAL_" + System.currentTimeMillis());
                conLocal.setHost(host);

                cddl = CDDL.getInstance();
                cddl.setContext(CDDLService.this);
                cddl.setConnection(conLocal);

                conLocal.addConnectionListener(new IConnectionListener() {
                    @Override
                    public void onConnectionEstablished() {
                        Log.i(TAG, "Conexão local com MicroBroker estabelecida.");
                        localConnectionReady = true;

                        cddlExecutor.execute(() -> {
                            startCdalServicesIfNeeded();
                            startCommandServiceImplIfNeeded();
                            startBleAcquisitionIfNeeded();
                        });
                    }

                    @Override
                    public void onConnectionEstablishmentFailed() {
                        Log.e(TAG, "Falha crítica ao conectar no MicroBroker local.");
                    }

                    @Override
                    public void onConnectionLost() {
                        Log.w(TAG, "Conexão com MicroBroker local perdida.");
                        localConnectionReady = false;
                    }

                    @Override
                    public void onDisconnectedNormally() {
                        Log.i(TAG, "Desconectado normalmente do MicroBroker local.");
                        localConnectionReady = false;
                    }
                });

                conLocal.connect();

            } catch (Exception e) {
                Log.e(TAG, "Erro ao inicializar MicroBroker/CDDL local: " + e.getMessage(), e);
            }
        });
    }

    private void startCdalServicesIfNeeded() {
        if (cdalStarted) {
            Log.w(TAG, "CDAL/S2PA já foi inicializada. Ignorando nova inicialização.");
            return;
        }

        cdalStarted = true;

        try {
            registerMHubListener();

            mainHandler.post(() -> {
                try {
                    startService(new Intent(CDDLService.this, AdaptationService.class));
                    startService(new Intent(CDDLService.this, S2PAService.class));
                    Log.i(TAG, "Serviços CDAL/S2PA iniciados.");
                } catch (Exception e) {
                    Log.e(TAG, "Erro ao iniciar serviços CDAL/S2PA: " + e.getMessage(), e);
                    cdalStarted = false;
                }
            });

        } catch (Exception e) {
            cdalStarted = false;
            Log.e(TAG, "Erro ao preparar CDAL/S2PA: " + e.getMessage(), e);
        }
    }

    private void startCommandServiceImplIfNeeded() {
        if (commandServiceStarted) {
            Log.w(TAG, "CommandServiceImpl já foi iniciado. Ignorando nova inicialização.");
            return;
        }

        if (!localConnectionReady || cddl == null || cddl.getConnection() == null) {
            Log.w(TAG, "CommandServiceImpl ainda não pode iniciar: conexão local indisponível.");
            return;
        }

        try {
            commandServiceImpl = new CommandServiceImpl();
            commandServiceStarted = true;
            Log.i(TAG, "CommandServiceImpl iniciado manualmente com conexão local válida.");
        } catch (Exception e) {
            commandServiceStarted = false;
            Log.e(TAG, "Erro ao iniciar CommandServiceImpl: " + e.getMessage(), e);
        }
    }

    private void startBleAcquisitionIfNeeded() {
        if (cddl == null) {
            Log.e(TAG, "CDDL nulo. Não foi possível iniciar BLE.");
            return;
        }

        try {
            /*
             * Pequena espera para permitir que AdaptationService e S2PAService
             * registrem seus listeners no MHubEventBus antes do startSensor.
             */
            Thread.sleep(1500);

            cddl.startCommunicationTechnology(CDDL.BLE_TECHNOLOGY_ID);
            cddl.startSensor(TARGET_SENSOR);

            Log.i(TAG, "BLE iniciado. Buscando módulos HMSoft.");
        } catch (Exception e) {
            Log.e(TAG, "Erro ao iniciar BLE/HMSoft: " + e.getMessage(), e);
        }
    }

    private void registerMHubListener() {
        try {
            if (!MHubEventBus.getDefault().isRegistered(this)) {
                MHubEventBus.getDefault().register(this);
                Log.d(TAG, "Listener registrado no MHubEventBus.");
            }
        } catch (Exception e) {
            Log.e(TAG, "Erro ao registrar listener no MHubEventBus: " + e.getMessage(), e);
        }
    }

    // ========================================================================
    // RECEPÇÃO DE DADOS DA CDAL/S2PA
    // ========================================================================

    @Subscribe(threadMode = ThreadMode.POSTING)
    public void onMessageEvent(SensorData sensorData) {
        if (sensorData == null) {
            return;
        }

        cddlExecutor.execute(() -> handleSensorData(sensorData));
    }

    private void handleSensorData(SensorData sensorData) {
        String action = sensorData.getAction();
        String mouuid = sensorData.getMouuid();

        if (SensorData.READ.equals(action)) {
            handleSensorRead(sensorData);
            return;
        }

        if (SensorData.DISCONNECTED.equals(action)) {
            if (mouuid == null) {
                return;
            }

            boolean wasKnownHmSoft = activeHmSoftMouuids.remove(mouuid);

            if (wasKnownHmSoft) {
                Log.w(TAG, "HMSoft conhecido desconectado: " + mouuid);
                scheduleHmSoftReacquisition();
            } else {
                Log.d(TAG, "Desconexão ignorada de dispositivo não confirmado como HMSoft: " + mouuid);
            }
        }
    }

    private void handleSensorRead(SensorData sensorData) {
        if (!TARGET_SENSOR.equals(sensorData.getSensorName())) {
            return;
        }

        Double[] values = sensorData.getSensorValue();

        if (values == null || values.length == 0) {
            return;
        }

        String mouuid = sensorData.getMouuid();

        if (mouuid != null && activeHmSoftMouuids.add(mouuid)) {
            Log.w(TAG, "HMSoft válido identificado por dados: " + mouuid);
        }

        SensorDataMessage message = new SensorDataMessage(sensorData);
        message.setQocEvaluated(true);
        message.setPublisherID(localPublisherId);
        message.setTopic("mhub/" + localPublisherId + "/service_topic/" + TARGET_SENSOR);

        publishSensorMessageToExternalBroker(message);
    }

    // ========================================================================
    // PUBLICAÇÃO DOS DADOS NO BROKER EXTERNO
    // ========================================================================

    private void publishSensorMessageToExternalBroker(Message message) {
        if (conExterno != null && conExterno.isConnected()) {
            if (publisherExterno == null) {
                publisherExterno = PublisherFactory.createPublisher();
                publisherExterno.addConnection(conExterno);
            }

            publisherExterno.publish(message);
            return;
        }

        long now = System.currentTimeMillis();
        if (now - lastBrokerUnavailableLogMs >= BROKER_UNAVAILABLE_LOG_INTERVAL_MS) {
            lastBrokerUnavailableLogMs = now;
            Log.w(TAG, "Broker externo indisponível. Mensagens HMSoft não estão sendo publicadas.");
        }
    }

    // ========================================================================
    // COMANDOS: BROKER EXTERNO -> MICROBROKER LOCAL -> COMMANDSERVICEIMPL
    // ========================================================================

    private void subscribeCommandFromExternalBroker() {
        if (conExterno == null || !conExterno.isConnected()) {
            Log.w(TAG, "Não foi possível assinar comandos: broker externo indisponível.");
            return;
        }

        if (subscriberCommandExternal != null) {
            return;
        }

        try {
            subscriberCommandExternal = SubscriberFactory.createSubscriber();
            subscriberCommandExternal.addConnection(conExterno);
            subscriberCommandExternal.subscribeCommandTopic();
            subscriberCommandExternal.setSubscriberListener(this::publishCommandMessageToMicroBroker);

            Log.i(TAG, "Assinatura de comandos no broker externo ativada.");
        } catch (Exception e) {
            subscriberCommandExternal = null;
            Log.e(TAG, "Erro ao assinar comandos no broker externo: " + e.getMessage(), e);
        }
    }

    private void publishCommandMessageToMicroBroker(Message message) {
        if (message == null) {
            return;
        }

        if (conLocal == null || !conLocal.isConnected()) {
            Log.w(TAG, "Comando recebido, mas MicroBroker local está indisponível.");
            return;
        }

        try {
            if (publisherLocal == null) {
                publisherLocal = PublisherFactory.createPublisher();
                publisherLocal.addConnection(conLocal);
            }

            publisherLocal.publish(message);
            Log.d(TAG, "Comando encaminhado do broker externo para o MicroBroker local.");

        } catch (Exception e) {
            Log.e(TAG, "Erro ao encaminhar comando para o MicroBroker local: " + e.getMessage(), e);
        }
    }

    // ========================================================================
    // RESET CONTROLADO DA CAMADA BLE
    // ========================================================================

    private void scheduleHmSoftReacquisition() {
        if (!BLE_RESET_ON_DISCONNECT_ENABLED) {
            return;
        }

        if (bleRestartInProgress) {
            Log.w(TAG, "Reset BLE já está em andamento. Ignorando nova solicitação.");
            return;
        }

        bleRestartInProgress = true;
        bleResetHandler.removeCallbacksAndMessages(null);

        Log.w(TAG, "Agendando reset controlado da camada BLE após desconexão do HMSoft.");

        bleResetHandler.postDelayed(() -> cddlExecutor.execute(() -> {
            try {
                Log.w(TAG, "Reset BLE - etapa 1: parando sensor HMSoft e tecnologia BLE.");

                if (cddl != null) {
                    try {
                        cddl.stopSensor(TARGET_SENSOR);
                    } catch (Exception e) {
                        Log.e(TAG, "Erro ao parar sensor HMSoft: " + e.getMessage());
                    }

                    try {
                        cddl.stopCommunicationTechnology(CDDL.BLE_TECHNOLOGY_ID);
                    } catch (Exception e) {
                        Log.e(TAG, "Erro ao parar tecnologia BLE: " + e.getMessage());
                    }
                }

                activeHmSoftMouuids.clear();

            } catch (Exception e) {
                Log.e(TAG, "Erro geral ao iniciar reset BLE: " + e.getMessage(), e);
            }

            bleResetHandler.postDelayed(() -> cddlExecutor.execute(() -> {
                try {
                    Log.w(TAG, "Reset BLE - etapa 2: reiniciando tecnologia BLE e sensor HMSoft.");

                    if (cddl != null) {
                        cddl.startCommunicationTechnology(CDDL.BLE_TECHNOLOGY_ID);
                        cddl.startSensor(TARGET_SENSOR);
                    }
                } catch (Exception e) {
                    Log.e(TAG, "Erro ao reiniciar BLE/HMSoft: " + e.getMessage(), e);
                } finally {
                    bleRestartInProgress = false;
                }
            }), BLE_RESTART_DELAY_MS);
        }), REACQUIRE_DELAY_MS);
    }

    // ========================================================================
    // CONEXÃO COM BROKER EXTERNO E WATCHDOG MQTT
    // ========================================================================

    private void initConnectExternalBroker() {
        cleanupExternalBrokerConnection();

        conExterno = ConnectionFactory.createConnection();
        conExterno.setClientId("ServiceLab_RPi_" + System.currentTimeMillis());
        conExterno.setHost(EXTERNAL_BROKER);
        conExterno.addConnectionListener(connectionListener);
        conExterno.setPublishConnectionChangedStatus(true);
        conExterno.connect();
    }

    private final IConnectionListener connectionListener = new IConnectionListener() {
        @Override
        public void onConnectionEstablished() {
            Log.i(TAG, "Conexão com broker externo (lsdi.ufma.br) estabelecida.");
            subscribeCommandFromExternalBroker();
        }

        @Override
        public void onConnectionEstablishmentFailed() {
            Log.e(TAG, "Falha na conexão com o broker externo.");

            showBrokerDisconnectedNotification(
                    "Falha ao conectar com lsdi.ufma.br. Tentando novamente em 10s."
            );

            triggerReconnectWatchdog();
        }

        @Override
        public void onConnectionLost() {
            Log.e(TAG, "Conexão com o broker externo perdida.");

            showBrokerDisconnectedNotification(
                    "Conexão com lsdi.ufma.br perdida. Tentando reconectar."
            );

            triggerReconnectWatchdog();
        }

        @Override
        public void onDisconnectedNormally() {
            Log.i(TAG, "Desconectado do broker externo de forma programada.");
        }
    };

    private void triggerReconnectWatchdog() {
        watchdogHandler.removeCallbacksAndMessages(null);

        watchdogHandler.postDelayed(() -> {
            Log.d(TAG, "Watchdog: reiniciando conexão com broker externo...");
            initConnectExternalBroker();
        }, 10000);
    }

    private void cleanupExternalBrokerConnection() {
        try {
            if (conExterno != null) {
                try {
                    conExterno.unsubscribeAll();
                } catch (Exception e) {
                    Log.w(TAG, "Falha ao remover assinaturas do broker externo: " + e.getMessage());
                }

                try {
                    conExterno.disconnect();
                } catch (Exception e) {
                    Log.w(TAG, "Falha ao desconectar broker externo antigo: " + e.getMessage());
                }
            }
        } catch (Exception e) {
            Log.e(TAG, "Erro ao limpar conexão externa antiga: " + e.getMessage(), e);
        }

        conExterno = null;
        publisherExterno = null;
        subscriberCommandExternal = null;
    }
}