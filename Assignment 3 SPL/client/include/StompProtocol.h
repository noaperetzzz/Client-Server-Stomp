#pragma once

#include <string> 
#include "event.h"

// TODO: implement the STOMP protocol
class StompProtocol
{
private:
public:
    static std::string connectFrame(const std::string& host, const std::string& login, const std::string& passcode);
    static std::string subscribeFrame(const std::string& destination, int subId, int receiptId);
    static std::string unsubscribeFrame(int subId, int receiptId);
    static std::string sendFrame(const std::string& destination, const std::string& username, const Event& event, const std::string json_path);
    static std::string disconnectFrame(int receiptId);
};
