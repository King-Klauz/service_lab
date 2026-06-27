package com.example.servicelab;

import android.Manifest;
import android.app.Notification;
import android.app.NotificationChannel;
import android.app.NotificationManager;
import android.app.PendingIntent;
import android.app.Service;
import android.content.Context;
import android.content.Intent;
import android.os.Build;
import android.os.Handler;
import android.os.IBinder;
import android.os.Looper;
import android.util.Log;

import androidx.annotation.Nullable;
import androidx.annotation.RequiresPermission;
import androidx.core.app.NotificationCompat;
import androidx.core.app.NotificationManagerCompat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import br.ufma.lsdi.cddl.CDDL;
import br.ufma.lsdi.cddl.ConnectionFactory;
import br.ufma.lsdi.cddl.listeners.IConnectionListener;
import br.ufma.lsdi.cddl.listeners.ISubscriberListener;
import br.ufma.lsdi.cddl.message.Message;
import br.ufma.lsdi.cddl.network.ConnectionImpl;
import br.ufma.lsdi.cddl.pubsub.Publisher;
import br.ufma.lsdi.cddl.pubsub.PublisherFactory;
import br.ufma.lsdi.cddl.pubsub.Subscriber;
import br.ufma.lsdi.cddl.pubsub.SubscriberFactory;

public class DataExchangeService extends Service {

    private static final String TAG = "DataExchangeService";
    public static final String CHANNEL_ID = "ServiceLAB";
    public static final String GROUP_KEY_FOREGROUND_SERVICE = "cddl_service_group";

    // IDs de Notificação Fixos para evitar vazamento de memória gráfica
    private static final int FOREGROUND_NOTIFICATION_ID = 1;
    private static final int STATUS_NOTIFICATION_ID = 2;

    private static final long RECONNECT_INTERVAL = 60000; // 1 minuto
    private static final long RECONNECT_DELAY_OFFSET = 5000; // 5 segundos

    private CDDL cddl;
    private ConnectionImpl arduinoConnection;
    private ConnectionImpl externalBrokerConnection;

    private Subscriber arduinoSubscriber;
    private Subscriber objectConnectedSubscriber;
    private Subscriber objectDisconnectedSubscriber;
    private Subscriber commandMessageSubscriber;

    private Publisher arduinoPublisher;
    private Publisher externalPublisher;

    // Thread-Safe list para evitar ConcurrentModificationException
    private final List<Object> envValues = Collections.synchronizedList(new ArrayList<>());

    private volatile boolean isConnected = false;

    // Instanciação explícita no MainLooper para evitar depreciação de API
    private final Handler connectionCheckHandler = new Handler(Looper.getMainLooper());

    private final Runnable connectionCheckRunnable = new Runnable() {
        @Override
        public void run() {
            if (!isConnected) {
                Log.w(TAG, "Instabilidade detectada no sensor. Reiniciando tecnologia BLE...");

                if (cddl != null) {
                    cddl.stopCommunicationTechnology(CDDL.BLE_TECHNOLOGY_ID);
                    CDDL.getInstance().stopSensor("HMSoft");
                }

                // Substituído o Thread.sleep por um agendamento não-bloqueante na fila de mensagens
                connectionCheckHandler.postDelayed(() -> {
                    if (cddl != null) {
                        cddl.startCommunicationTechnology(CDDL.BLE_TECHNOLOGY_ID);
                        CDDL.getInstance().startSensor("HMSoft");
                        Log.i(TAG, "Tecnologia BLE reiniciada. Próxima checagem agendada.");
                    }
                }, RECONNECT_DELAY_OFFSET);

                // Reagenda o ciclo principal de monitoramento
                connectionCheckHandler.postDelayed(this, RECONNECT_INTERVAL);
            }
        }
    };

    @RequiresPermission(Manifest.permission.POST_NOTIFICATIONS)
    @Override
    public void onCreate() {
        super.onCreate();
        createNotificationChannel();
        initCDDLServices();

        Intent notificationIntent = new Intent(this, MainActivity.class);
        PendingIntent pendingIntent = PendingIntent.getActivity(
                this, 0, notificationIntent, PendingIntent.FLAG_IMMUTABLE);

        Notification notification = new NotificationCompat.Builder(this, CHANNEL_ID)
                .setContentTitle("Serviço em execução")
                .setContentText("O monitoramento está ativo.")
                .setSmallIcon(R.drawable.logo_lsdi_round)
                .setContentIntent(pendingIntent)
                .setCategory(NotificationCompat.CATEGORY_SERVICE)
                .setGroup(GROUP_KEY_FOREGROUND_SERVICE)
                .setPriority(NotificationCompat.PRIORITY_MAX)
                .setOngoing(true)
                .setAutoCancel(false)
                .build();

        startForeground(FOREGROUND_NOTIFICATION_ID, notification);
    }

    @RequiresPermission(Manifest.permission.POST_NOTIFICATIONS)
    private void initCDDLServices() {
        initCDDL();
        subscribeObjectConnected();
        subscribeObjectDisconnected();
        initConnectExternalBroker();
        subscribeEnvironmentalSensingFromArduino();
        subscribeCommandMessageFromExternalBroker();

        // Remove callbacks idênticos pré-existentes para evitar loops concorrentes sobrepostos
        connectionCheckHandler.removeCallbacks(connectionCheckRunnable);
        connectionCheckHandler.postDelayed(connectionCheckRunnable, RECONNECT_INTERVAL);
    }

    private void subscribeObjectConnected() {
        if (objectConnectedSubscriber == null) {
            objectConnectedSubscriber = SubscriberFactory.createSubscriber();
            objectConnectedSubscriber.addConnection(arduinoConnection);
            objectConnectedSubscriber.subscribeObjectConnectedTopic();
            objectConnectedSubscriber.setSubscriberListener(message -> {
                Log.d(TAG, "Conectado ao módulo HMSoft.");
                sendMessageNotification("Connected to HMSoft");
                isConnected = true;
                connectionCheckHandler.removeCallbacks(connectionCheckRunnable);
            });
        }
    }

    private void subscribeObjectDisconnected() {
        if (objectDisconnectedSubscriber == null) {
            objectDisconnectedSubscriber = SubscriberFactory.createSubscriber();
            objectDisconnectedSubscriber.addConnection(arduinoConnection);
            objectDisconnectedSubscriber.subscribeObjectDisconnectedTopic();
            objectDisconnectedSubscriber.setSubscriberListener(message -> {
                Log.w(TAG, "Conexão perdida com o módulo HMSoft.");
                sendMessageNotification("Disconnected from HMSoft");
                isConnected = false;

                // Força o agendamento imediato e limpa a fila antiga
                connectionCheckHandler.removeCallbacks(connectionCheckRunnable);
                connectionCheckHandler.post(connectionCheckRunnable);
            });
        }
    }

    private void sendMessageNotification(String msg) {
        try {
            Notification notification = new NotificationCompat.Builder(this, CHANNEL_ID)
                    .setContentTitle("Mensagem do Service")
                    .setContentText(msg)
                    .setSmallIcon(R.drawable.ic_launcher_foreground)
                    .setPriority(NotificationCompat.PRIORITY_DEFAULT)
                    .setAutoCancel(true)
                    .build();

            NotificationManagerCompat manager = NotificationManagerCompat.from(this);
            // Verificação explícita de permissão exigida pelas APIs modernas do Android
            if (Build.VERSION.SDK_INT < Build.VERSION_CODES.TIRAMISU ||
                    checkSelfPermission(Manifest.permission.POST_NOTIFICATIONS) == android.content.pm.PackageManager.PERMISSION_GRANTED) {
                manager.notify(STATUS_NOTIFICATION_ID, notification);
            }
        } catch (Exception e) {
            Log.e(TAG, "Falha ao disparar notificação: " + e.getMessage());
        }
    }

    @Override
    public int onStartCommand(Intent intent, int flags, int startId) {
        return START_STICKY;
    }

    @Override
    public void onDestroy() {
        // CORREÇÃO CRÍTICA: Desvincula o Handler para evitar vazamento de memória ao encerrar o app
        connectionCheckHandler.removeCallbacksAndMessages(null);
        stopCDDLServices();
        super.onDestroy();
    }

    private void stopCDDLServices() {
        if (cddl != null) {
            try {
                cddl.stopAllSensors();
                cddl.stopAllCommunicationTechnologies();
                cddl.stopService();
            } catch (Exception e) {
                Log.e(TAG, "Erro ao parar serviços do CDDL: " + e.getMessage());
            }

            if (arduinoConnection != null) {
                arduinoConnection.unsubscribeAll();
                arduinoConnection.disconnect();
                arduinoConnection = null;
            }

            if (externalBrokerConnection != null) {
                externalBrokerConnection.unsubscribeAll();
                externalBrokerConnection.disconnect();
                externalBrokerConnection = null;
            }

            commandMessageSubscriber = null;
            arduinoSubscriber = null;
            arduinoPublisher = null;
            externalPublisher = null;
            objectConnectedSubscriber = null;
            objectDisconnectedSubscriber = null;

            CDDL.stopMicroBroker();
            cddl = null;
        }
    }

    @Nullable
    @Override
    public IBinder onBind(Intent intent) {
        return null;
    }

    private void createNotificationChannel() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            NotificationChannel serviceChannel = new NotificationChannel(
                    CHANNEL_ID,
                    "CDDL Service Lab",
                    NotificationManager.IMPORTANCE_HIGH
            );
            serviceChannel.setDescription("Canal para notificações do Serviço CDDL");
            NotificationManager manager = (NotificationManager) getSystemService(Context.NOTIFICATION_SERVICE);
            if (manager != null) {
                manager.createNotificationChannel(serviceChannel);
            }
        }
    }

    private void initCDDL() {
        String host = CDDL.startMicroBroker();
        arduinoConnection = ConnectionFactory.createConnection();
        arduinoConnection.setClientId("MHUB_SALAS");
        arduinoConnection.setHost(host);
        arduinoConnection.connect();

        cddl = CDDL.getInstance();
        cddl.setConnection(arduinoConnection);
        cddl.setContext(this);
        cddl.startService();
        cddl.startCommunicationTechnology(CDDL.BLE_TECHNOLOGY_ID);
        CDDL.getInstance().startSensor("HMSoft");
    }

    private void initConnectExternalBroker() {
        String host = "lsdi.ufma.br";
        externalBrokerConnection = ConnectionFactory.createConnection();
        externalBrokerConnection.setClientId("MHUB_SALAS");
        externalBrokerConnection.setHost(host);
        externalBrokerConnection.addConnectionListener(connectionListener);
        externalBrokerConnection.setPublishConnectionChangedStatus(true);
        externalBrokerConnection.connect();
    }

    private void subscribeEnvironmentalSensingFromArduino() {
        if (arduinoSubscriber == null) {
            arduinoSubscriber = SubscriberFactory.createSubscriber();
            arduinoSubscriber.addConnection(arduinoConnection);
            arduinoSubscriber.subscribeServiceByName("HMSoft");
            arduinoSubscriber.setSubscriberListener(message -> {
                Log.d(TAG, "Dados recebidos do Arduino: " + message);
                Object[] serviceValue = message.getServiceValue();
                if (serviceValue == null) return;

                boolean firstMessage = true;

                // Sincronização do bloco para proteger a lista compartilhada entre as threads
                synchronized (envValues) {
                    for (Object value : serviceValue) {
                        if (value instanceof Number) {
                            // Cast seguro utilizando a superclasse Number para evitar ClassCastException
                            double v = ((Number) value).doubleValue();
                            if (v == -1) {
                                firstMessage = false;
                            }
                        }
                    }

                    if (firstMessage) {
                        Log.d(TAG, "Tratando primeira metade do payload do pacote.");
                        envValues.addAll(Arrays.asList(serviceValue));
                    } else {
                        if (!envValues.isEmpty()) {
                            Log.d(TAG, "Tratando segunda metade do payload. Consolidando mensagem...");
                            envValues.addAll(Arrays.asList(serviceValue));
                            envValues.remove(envValues.size() - 1);

                            Message newmsg = new Message();
                            newmsg.setTopic(message.getTopic());
                            newmsg.setServiceName(message.getServiceName());
                            newmsg.setMouuid(message.getMouuid());
                            newmsg.setServiceValue(envValues.toArray());

                            publishArduinoDataToExternalBroker(newmsg);
                            envValues.clear();
                        }
                    }
                }
            });
        }
    }

    private void publishArduinoDataToExternalBroker(Message message) {
        Log.d(TAG, "Publicando mensagem consolidada no broker externo: " + message);
        if (externalPublisher == null) {
            externalPublisher = PublisherFactory.createPublisher();
            externalPublisher.addConnection(externalBrokerConnection);
        }
        externalPublisher.publish(message);
    }

    private void subscribeCommandMessageFromExternalBroker() {
        if (commandMessageSubscriber == null) {
            commandMessageSubscriber = SubscriberFactory.createSubscriber();
            commandMessageSubscriber.addConnection(externalBrokerConnection);
            commandMessageSubscriber.subscribeCommandTopic();
            commandMessageSubscriber.setSubscriberListener(message -> {
                Log.d(TAG, "Comando vindo do broker externo: " + message);
                publishCommandMessageToArduino(message);
            });
        }
    }

    private void publishCommandMessageToArduino(Message message) {
        if (arduinoPublisher == null) {
            arduinoPublisher = PublisherFactory.createPublisher();
            arduinoPublisher.addConnection(arduinoConnection);
        }
        arduinoPublisher.publish(message);
    }

    private final IConnectionListener connectionListener = new IConnectionListener() {
        @Override
        public void onConnectionEstablished() {
            Log.i(TAG, "Conexão com broker externo (lsdi.ufma.br) estabelecida.");
            sendMessageNotification("Conectado ao broker externo.");
        }

        @Override
        public void onConnectionEstablishmentFailed() {
            Log.e(TAG, "Falha na conexão com o broker externo.");
            sendMessageNotification("Falha ao conectar ao broker externo.");
        }

        @Override
        public void onConnectionLost() {
            Log.w(TAG, "Conexão com o broker externo perdida.");
            sendMessageNotification("A conexão ao broker externo foi perdida.");
        }

        @Override
        public void onDisconnectedNormally() {
            Log.i(TAG, "Desconexão amigável executada.");
            sendMessageNotification("Desconectado normalmente do broker externo.");
        }
    };
}