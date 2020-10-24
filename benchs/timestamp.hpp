#pragma once

#include <chrono>

class Timestamp {
    using time_point_t = std::chrono::time_point<std::chrono::high_resolution_clock>;
public:
	Timestamp() : start_(now())
    {}
    
    void reset()
    {
        start_ = now();
    }

	double elapsed_seconds() const
    {
		return std::chrono::duration_cast<std::chrono::seconds>(get_duration_from_now()).count();
    }

	double elapsed_millis() const
    {
		return std::chrono::duration_cast<std::chrono::milliseconds>(get_duration_from_now()).count();
	}

	double elapsed_micros() const
    {
		return std::chrono::duration_cast<std::chrono::microseconds>(get_duration_from_now()).count();
	}
private:
    std::chrono::duration<double> get_duration_from_now() const
    {
        auto end = now();
        return end - start_;
    }

    static time_point_t now() noexcept
    {
        return std::chrono::high_resolution_clock::now();
    }
private:
    time_point_t start_;
};
