#include "StompProtocol.h"
#include <sstream>

    std::string StompProtocol::connectFrame(const std::string& host, const std::string& login, const std::string& passcode) {
        std::stringstream ss;
        ss << "CONNECT\n"
        << "accept-version:1.2\n"
        << "host:" << host << "\n"
        << "login:" << login << "\n"
        << "passcode:" << passcode << "\n"
        << "\n";
        return ss.str();
    }

    std::string StompProtocol::subscribeFrame(const std::string& destination, int subId, int receiptId) {
        std::stringstream ss;
        ss << "SUBSCRIBE\n"
        << "destination:/" << destination << "\n"
        << "id:" << subId << "\n"
        << "receipt:" << receiptId << "\n"
        << "\n";
        return ss.str();
    }

    std::string StompProtocol::unsubscribeFrame(int subId, int receiptId) {
        std::stringstream ss;
        ss << "UNSUBSCRIBE\n"
        << "id:" << subId << "\n"
        << "receipt:" << receiptId << "\n"
        << "\n";
        return ss.str();
    }

    std::string StompProtocol::sendFrame(const std::string& destination, const std::string& username, const Event& event, const std::string json_path) {
        std::stringstream ss;
        ss << "SEND\n"
        << "destination:/" << destination << "\n\n"
        << "user: " << username << "\n"
        << "team a: " << event.get_team_a_name() << "\n"
        << "team b: " << event.get_team_b_name() << "\n"
        << "event name: " << event.get_name() << "\n"
        << "time: " << event.get_time() << "\n";

        ss << "general game updates:\n";
        for (const auto& update : event.get_game_updates()) {
            ss << "    " << update.first << ": " << update.second << "\n";
        }

        ss << "team a updates:\n";
        for (const auto& update : event.get_team_a_updates()) {
            ss << "    " << update.first << ": " << update.second << "\n";
        }

        ss << "team b updates:\n";
        for (const auto& update : event.get_team_b_updates()) {
            ss << "    " << update.first << ": " << update.second << "\n";
        }

        ss << "description: " << event.get_discription() << "\n";
        ss << "file name: " << json_path;
        return ss.str();
    }

    std::string StompProtocol::disconnectFrame(int receiptId) {
        std::stringstream ss;
        ss << "DISCONNECT\n"
        << "receipt:" << receiptId << "\n"
        << "\n";
        return ss.str();
    }
