class CommandBuffer
  attr_accessor :data_change_history, :count_change_history, :blocks, :affected_data, :block_counter

  def initialize
    @data_change_history  ||= {}
    @count_change_history ||= {}
    @affected_data        ||= []
    @blocks ||= []
    @block_counter = -1
  end

  def process(operation, key, value)
    if @block_counter != -1
      puts "BUFFER MODE #{@block_counter}"
    else
      puts "NORMAL #{@block_counter}"
    end

    if @block_counter == -1 && operation != "BEGIN"
      CommandExecuter.new(operation, key, value).excute
    else
      if operation == "SET" || operation == "UNSET"
        in_transaction_process(operation, key, value, @block_counter)
      elsif operation == "GET"
        puts "data change history is emtpy" if data_change_history.nil?
        get_from_buffer(key)
      elsif operation == "NUMEQUALTO"
        puts "the key for numequalto_from_buffer is #{key}"
        numequalto_from_buffer(key)
      elsif operation == "COMMIT"
        commit
        @block_counter = -1

      elsif operation == "ROLLBACK"
        if @block_counter == -1
          "NO TRANSACTION"
        else
          rollback
          @block_counter -= 1
        end
      elsif operation == "BEGIN"
        @block_counter += 1
        puts "current count: #{@block_counter}"
      end
    end
  end

  def in_transaction_process(operation, key, value, block_index)
    if operation == "SET" || operation == "UNSET"

      affected_data[block_index] = [] if affected_data[block_index].nil?
      affected_data[block_index] |= [key]

      data_change_history[key.to_sym] = [] if data_change_history[key.to_sym].nil?
      history_for_current_key = data_change_history[key.to_sym]
      data_previous_index = history_for_current_key.empty? ? -1 : history_for_current_key.last[:block_index]

      if block_index != data_previous_index
        history_for_current_key << { value: value, block_index: block_index }
        puts "affected_data: for block_index: #{block_index} key: #{key} #{affected_data[block_index]}"
      
      else
        history_for_current_key.last[:value] = value
         #= { value: value, block_index: block_index }
      end

      count_key = find_count_key(history_for_current_key, key)
      #if history_for_current_key.last[:value].nil?
      #  count_key = value
      #else
      #  count_key = history_for_current_key.last[:value]
      #end
      puts "count_key: #{count_key}"
      puts "value: #{value}, history_for_current_key: #{history_for_current_key}"
      count_change_history[count_key.to_sym] = [] if count_change_history[count_key.to_sym].nil?
      count_history_for_current_key = count_change_history[count_key.to_sym]
      count_previous_index = count_history_for_current_key.empty? ? -1 : count_history_for_current_key.last[:block_index]

      if block_index != count_previous_index
        puts "count_history_for_current_key: #{count_history_for_current_key}"
        last_count_change = count_history_for_current_key.empty? ? 0 : count_history_for_current_key.last[:diff]

        count_change_history[count_key.to_sym] = [] if count_change_history[count_key.to_sym].nil?
        if operation == "SET"
          count_change_history[count_key.to_sym] << { diff: last_count_change + 1, block_index: block_index }
        else
          count_change_history[count_key.to_sym] << { diff: last_count_change - 1, block_index: block_index } 
        end
        puts "count_key: #{count_key}, count_change_history[count_key.to_sym]: #{count_change_history[count_key.to_sym]}"
      else
        len = count_history_for_current_key.length
        last_count_change = count_history_for_current_key[len-2][:diff]
        if operation == "SET"
          count_change_history[count_key.to_sym].last[:diff] = last_count_change + 1
        else
          count_change_history[count_key.to_sym].last[:diff] = last_count_change - 1
        end
        puts "count_change_history[count_key.to_sym]: #{count_change_history[count_key.to_sym]}"        
        puts "history_for_current_key.last: #{history_for_current_key.last}"
        puts "history_for_current_key: #{history_for_current_key}"
        puts "count_key: #{count_key}, count_change_history[count_key.to_sym]: #{count_change_history[count_key.to_sym]}"
      end
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
    if data_change_history[key.to_sym].nil?
      CommandExecuter.new("GET", key).excute
    else
      puts "data_change_history[key.to_sym]: #{data_change_history[key.to_sym]}"
      puts data_change_history[key.to_sym].last[:value]
    end
  end

  def numequalto_from_buffer(key)
    initial_count = CommandExecuter.new("NUMEQUALTO", key).excute
    if count_change_history[key.to_sym].nil?
      puts initial_count
    else
      puts initial_count + count_change_history[key.to_sym].last[:diff]
    end
  end

  def find_count_key(history_for_current_key, key, block_index)
    len = history_for_current_key.length
    all_count_keys = []
    if len < 2
      all_count_keys << CommandExecuter.new("GET", key).excute

    i =  history_for_current_key.length - 2
    while i>=0 
      && !history_for_current_key[i].nil?
      && !history_for_current_key[i][:value].nil?
      && block_index == history_for_current_key[i][:block_index]
      && 
    while i >= 0 && history_for_current_key[i][:value].nil?
      i -= 1
    end

    i < 0 ? CommandExecuter.new("GET", key).excute : history_for_current_key[i][:value]
  end
end
