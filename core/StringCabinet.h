namespace cabinet {
  struct StringKeyHeaderExtractor : public std::unary_functional<uint32_t, std::string> {
    uint32_t operator()(std::string& str) const {
      return htole32((uint32_t)str.size());
    } 
  };

  struct 
}
