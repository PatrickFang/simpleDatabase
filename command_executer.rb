class CommandExecuter
  attr_reader :operation, :key, :value

  def initialize(operation, key, value=0)
    @operation ||= operation
    @key       ||= key
    @value     ||= value
  end

  def excute
    case operation
    when "SET"
      $db.set(key, value)
      $db.create_or_update_file(key, value)

      $db.set(value, 0, false)
      new_count = $db.get(value, false)
      $db.create_or_update_file(value, new_count, false)
    when "GET"
      return $db.get(key)
    when "UNSET"
      if $db.get(key) != nil
        count_key = $db.get(key)
        $db.unset(count_key.to_s, false)
        $db.unset(key)

        $db.delete_data_file(key)

        current_count = $db.get(count_key, false)
        $db.create_or_update_file(count_key, current_count, false)
      else
        "DOES NOT EXIST"
      end
    when "NUMEQUALTO"
      return $db.get(key, false)
    else 
      'UNKNOWN COMMAND'
    end
  end
end
