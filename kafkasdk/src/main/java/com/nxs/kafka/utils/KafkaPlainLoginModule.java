package com.nxs.kafka.utils;

import com.huateng.ms4.key.SecreKey;
import com.huateng.ms4.util.SM4Util;
import org.apache.kafka.common.security.plain.PlainSaslServerProvider;

import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;
import java.util.Map;

/**
 * Created by songyu on 2018/8/28.
 */
public class KafkaPlainLoginModule implements LoginModule  {

    private String username;
    private String password;

    public KafkaPlainLoginModule(){

    }

    public KafkaPlainLoginModule(String username, String password) {
        this.username = username;
        this.password = password;
    }

    public void initialize(Subject subject, CallbackHandler callbackHandler, Map<String, ?> sharedState, Map<String, ?> options) {
        String username = (String)options.get("username");
        if(username != null) {
            subject.getPublicCredentials().add(username);
        }

        String password = (String)options.get("password");
        if(password != null) {
            try {
                password = SM4Util.decryptData(password, SecreKey.SECRET_Key);
            } catch (Exception e) {
                e.printStackTrace();
            }
            subject.getPrivateCredentials().add(password);
        }

    }

    public boolean login() throws LoginException {
        return true;
    }

    public boolean logout() throws LoginException {
        return true;
    }

    public boolean commit() throws LoginException {
        return true;
    }

    public boolean abort() throws LoginException {
        return false;
    }

    static {
        PlainSaslServerProvider.initialize();
    }
}
