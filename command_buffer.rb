class CommandBuffer
  attr_accessor :data_change_history, :count_change_history, :blocks, :affected_data

  def initialize
    @data_change_history  ||= {}
    @count_change_history ||= {}
    @affected_data        ||= []
    @blocks ||= []
  end

  def process(operation, key, value, block_index)
    if operation.in?("SET", "UNSET")
      affected_data[block_index] |= [key]
      if block_index != data_change_history[key.to_sym].last[:block_index]
        data_change_history[key.to_sym] << { value: value, block_index: block_index }

        affected_data[block_index].each do |key|
          count_key = data_change_history[key.to_sym]
          history = count_change_history[count_key.to_sym]
            << { diff: history.last[:diff] + 1, block_index: block_index }
        end
      else
        data_change_history[key.to_sym].last = { value: value, block_index: block_index }
        affected_data[block_index].each do |key|
          count_key = data_change_history[key.to_sym]

          history = count_change_history[count_key.to_sym]
            << { diff: history.last[:diff] - 1, block_index: block_index }
        end
      end
    elsif operation == "GET"
      get_from_buffer(key)
    elsif operation == "NUMEQUALTO"
      numequalto_from_buffer(key)
    elsif operation == "COMMIT"
      commit
    elsif operation == "ROLLBACK"
      rollback
    end
  end

  def commit
    data_change_history.each do |key, change|
      if change.last[:value].nil?
        CommandExecuter.new("UNSET", key).excute
      else
        CommandExecuter.new("SET", key, change.last[:value]).excute
      end
    end
  end

  def rollback
    affected_data.last.each do |key|
      data_change_history[key.to_sym].pop
    end
  end

  def get_from_buffer(key)
    if data_change_history[key.to_sym].nil
      CommandExecuter.new("GET", key).excute
    else
      data_change_history[key.to_sym].last[:value]
    end
  end

  def numequalto_from_buffer(key)


  end

  def get_initial_count_from_db
  end
end